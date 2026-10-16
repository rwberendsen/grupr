package snowflake

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"iter"
	"log"
	"net/url"
	"strings"

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

func (o AggAccountObjs) archive(ctx context.Context, semCnf *semantics.Config, cnf *Config, conn *sql.DB, path []string, interfaceID string) ([]string, error) {
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
				pathStr, err := url.JoinPath("", pathObj...)
				if err != nil {
					return failedQueries, fmt.Errorf("archive: %w", err)
				}

				// Get the columns and their data types from the information schema,
				// and generate SQL to do the timestamp conversion.
				var cols []Column
				cols, err = objAttr.getColumns(ctx, conn, db, schema, obj)
				if err != nil {
					return failedQueries, err
				}
				colNames := map[semantics.Ident]bool{}
				for _, col := range cols {
					colNames[col.Name] = true
				}

				// Make a pass over the colums and do the type conversions
				// Note that in the case of a TIMESTAMP_TZ we want to insert a column here with the offset.
				var colLines []string
				objCanBeSerialized := true
				var blockers []string
				for _, col := range cols {
					switch col.DataType {
					case "TIMESTAMP_LTZ":
						// In this case, we argue that the data does not contain an offset: the offset is only determined
						// at read time by the active time zone in the session in Snowflake.
						// We adopt the approach of converting to UTC.
						// If a non UTC time zone would be required to perform some computation on the timestamps here,
						// the consumer of this data should select the appropriate timezone.
						colLines = append(colLines, fmt.Sprintf("CONVERT_TIMEZONE('UTC', %s))::TIMESTAMP_NTZ AS %s", col.Name, col.Name))
					case "TIMESTAMP_TZ":
						// In this case, the data does contain an offset. While this does not directly give us an actual
						// time zone, it is still information we do not simply want to discard. We (attempt to) add two
						// columns to the schema of the to-be serialized Parquet file(s).  We tell Snowflake to store the
						// offset in the new columns, and the wall-clock time part in the original column.
						tzHrCol := col.Name + semCnf.Infix + semantics.Ident("TZ_HR")
						tzMinCol := col.Name + semCnf.Infix + semantics.Ident("TZ_MIN")
						if colNames[tzHrCol] || colNames[tzMinCol] {
							objCanBeSerialized = false
							colLines = append(colLines, fmt.Sprintf("  , %s", col.Name))
							if colNames[tzHrCol] {
								blockers = append(blockers, fmt.Sprintf("column %s already exists", tzHrCol.SingleQuote()))
							}
							if colNames[tzMinCol] {
								blockers = append(blockers, fmt.Sprintf("column %s already exists", tzMinCol.SingleQuote()))
							}
						} else {
							colLines = append(colLines, fmt.Sprintf("%s::TIMESTAMP_NTZ                AS %s", col.Name, col.Name))
							colLines = append(colLines, fmt.Sprintf("EXTRACT(timezone_hour FROM %s)   AS %s", col.Name, tzHrCol))
							colLines = append(colLines, fmt.Sprintf("EXTRACT(timezone_minute FROM %s) AS %s", col.Name, tzMinCol))
						}
					case "OBJECT", "ARRAY":
						objCanBeSerialized = false
						colLines = append(colLines, fmt.Sprintf("%s", col.Name))
						blockers = append(blockers, fmt.Sprintf("column %s: unsupported data type %s", col.Name, col.DataType))
					default:
						colLines = append(colLines, fmt.Sprintf("%s", col.Name))
					}
				}

				comment := "-- Unloading:\n"
				if !objCanBeSerialized {
					comment = "-- The query below needs customization. Blockers:\n"
					for blocker := range blockers {
						comment += fmt.Sprintf("-- %s\n", blocker)
					}
				}
				columnClause := strings.Join(colLines, "\n  , ")

				sql := fmt.Sprintf(`%sCOPY INTO @%s.%s.%s/%s
FROM (SELECT 
    %s
FROM IDENTIFIER($$%s$$))
INCLUDE_QUERY_ID = TRUE
DETAILED_OUTPUT = TRUE
HEADER = TRUE`, comment, cnf.Database, cnf.Schema, cnf.ExternalWriteStage, pathStr, columnClause, objAttr.ObjectType.FQN(db, schema, obj))

				if !objCanBeSerialized {
					failedQueries = append(failedQueries, sql)
					continue
				}

				if err := runSQL(ctx, cnf, conn, sql); err != nil {
					if sfErr, ok := errors.AsType[*gosnowflake.SnowflakeError](err); ok {
						log.Printf("SnowflakeError:\n")
						log.Printf("Number:   %d\n", sfErr.Number)
						log.Printf("SQLState: %s\n", sfErr.SQLState)
						log.Printf("QueryID:  %s\n", sfErr.QueryID)
						log.Printf("Message:  %s\n", sfErr.Message)
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
							continue
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
