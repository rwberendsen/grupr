package snowflake

import (
	"context"
	"database/sql"
	"iter"

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
