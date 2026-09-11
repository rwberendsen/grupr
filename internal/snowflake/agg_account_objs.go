package snowflake

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"iter"
	"log"
	"net/url"

	"github.com/rwberendsen/grupr/internal/semantics"
	"github.com/snowflakedb/gosnowflake"
)

// AccountObjs aggregated to (product, dtap, interface) level, with fields to store granted privileges on them
type AggAccountObjs struct {
	DBs map[semantics.Ident]AggDBObjs
}

func newAggAccountObjs(o AccountObjs) AggAccountObjs {
	r := AggAccountObjs{DBs: make(map[semantics.Ident]AggDBObjs, len(o.DBs))}
	for db, dbObjs := range o.DBs {
		r.DBs[db] = newAggDBObjs(dbObjs)
	}
	return r
}

func (o AggAccountObjs) GetSchema(db semantics.Ident, schema semantics.Ident) (schemaObjs AggSchemaObjs, ok bool) {
	if dbObjs, okay := o.DBs[db]; okay {
		schemaObjs, ok = dbObjs.Schemas[schema]
	}
	return
}

func (o AggAccountObjs) GetObject(db semantics.Ident, schema semantics.Ident, obj semantics.Ident) (objAttr AggObjAttr, ok bool) {
	if dbObjs, okay := o.DBs[db]; okay {
		if schemaObjs, okay := dbObjs.Schemas[schema]; okay {
			objAttr, ok = schemaObjs.Objects[obj]
		}
	}
	return
}

func (o AggAccountObjs) getExternalGrants(ctx context.Context, semCnf *semantics.Config, conn *sql.DB) iter.Seq2[Grant, error] {
	return func(yield func(Grant, error) bool) {
		for db, dbObjs := range o.DBs {
			if !dbObjs.pushExternalGrants(ctx, semCnf, conn, db, yield) {
				return
			}
		}
	}
}

func (o AggAccountObjs) archive(ctx context.Context, cnf *Config, conn *sql.DB, path []string, interfaceID string) ([]string, error) {
	failedQueries := []string{}
	pathInterface := append(path)
	if interfaceID != "" {
		pathInterface = append(pathInterface, "interfaces", interfaceID)
	}
	for db, dbObjs := range o.DBs {
		for schema, schemaObjs := range dbObjs.Schemas {
			for obj, objAttr := range schemaObjs.Objects {
				pathObj := append(pathInterface, "dbs", string(db), "schemas", string(schema), "objects", string(obj))
				for i := range pathObj {
					pathObj[i] = url.PathEscape(pathObj[i])
				}
				if pathStr, err := url.JoinPath("", pathObj...); err != nil {
					return failedQueries, fmt.Errorf("archive: %w", err)
				} else {
					sql := fmt.Sprintf(`COPY INTO @%s.%s.%s/%s
FROM (SELECT * FROM IDENTIFIER($$%s$$))
INCLUDE_QUERY_ID = TRUE
DETAILED_OUTPUT = TRUE
HEADER = TRUE`, cnf.Database, cnf.Schema, cnf.ExternalWriteStage, pathStr, objAttr.ObjectType.FQN(db, schema, obj))
					if err := runSQL(ctx, cnf, conn, sql); err != nil {
						if sfErr, ok := errors.AsType[*gosnowflake.SnowflakeError](err); ok {
							log.Printf("SnowflakeError:\n")
							log.Printf("Number:   %d\n", sfErr.Number)
							log.Printf("SQLState: %d\n", sfErr.SQLState)
							log.Printf("QueryID:  %d\n", sfErr.QueryID)
							log.Printf("Message:   %d\n", sfErr.Message)
							// For some particular error numbers we want to skip this object,
							// continue archiving the other objects, and notify the user
							// at the end about the failed queries, where some customization of
							// our generic COPY INTO @location statement would be required.
							if sfErr.Number == 100171 {
								// Some columns contained data types not directly compatible with Parquet,
								// i.e., we've observed Snowflake reporting this error number when
								// complaining about TIMESTAMP_TZ or TIMESTAMP_LTZ columns that it would
								// not convert to any Parquet data type
								failedQueries = append(failedQueries, sql)
							} else {
								return failedQueries, err
							}
						} else {
							return failedQueries, err
						}
					}
				}
			}
		}
	}
	return failedQueries, nil
}

func (o AggAccountObjs) purge(ctx context.Context, cnf *Config, conn *sql.DB) error {
	for db, dbObjs := range o.DBs {
		if err := dbObjs.purge(ctx, cnf, conn, db); err != nil {
			return err
		}
	}
	return nil
}
