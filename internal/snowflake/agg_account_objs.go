package snowflake

import (
	"context"
	"database/sql"
	"iter"
	"net/url"

	"github.com/rwberendsen/grupr/internal/semantics"
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

func (o AggAccountObjs) archive(ctx context.Context, cnf *Config, conn *sql.DB, path, isProd bool, dtap string, interfaceID string) error {
	prodOrNot := map[bool]string{true: "prod", false: "non-prod"}
	path += fmt.Sprintf("%s/dtaps/%s/", prodOrNot, dtap)
	if interfaceID != "" {
		path += fmt.Sprintf("interfaces/%s/", interfaceID)
	}
	for db, dbObjs := range o.DBs {
		for schema, schemaObjs := range dbObjs.Schemas {
			for obj, objAttr := range schemaObjs.Objects {
				path += fmt.Sprintf("dbs/%s/schemas/%s/objects/%s/", db, schema, obj)
				if err := runSQL(ctx, cnf, conn, fmt.Sprintf(`COPY INTO @%s.%s.%s/%s
FROM (SELECT * FROM IDENTIFIER($$%s$$))
INCLUDE_QUERY_ID = TRUE
DETAILED_OUTPUT = TRUE
HEADER = TRUE
`, cnf.Database, cnf.Schema, cnf.ExternalWriteStage, url.PathEscape(path), objAttr.ObjectType.FQN(db, schema, obj))); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

func (o AggAccountObjs) purge(ctx context.Context, cnf *Config, conn *sql.DB) error {
	for db, dbObjs := range o.DBs {
		if err := dbObjs.purge(ctx, cnf, conn, db); err != nil {
			return err
		}
	}
	return nil
}
