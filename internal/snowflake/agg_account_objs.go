package snowflake

import (
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

func (o AggAccountObjss) GetSchema(db semantics.Ident, schema semantics.Ident) AggObjAttr, bool {
	if dbObjs, ok := DBs[db]; ok {
		return dbObjs.Schemas[schema]
	}
	return AggSchemaObjs{}, false
}

func (o AggAccountObjss) GetObject(db semantics.Ident, schema semantics.Ident, obj semantics.Ident) AggObjAttr, bool {
	if dbObjs, ok := DBs[db]; ok {
		if schemaObjs, ok := dbObjs.Schemas[schema]; ok {
			return schemaObjs.Objects[obj] {
		}
	}
	return AggObjAttr{}, false
}
