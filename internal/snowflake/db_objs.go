package snowflake

import (
	"github.com/rwberendsen/grupr/internal/semantics"
)

type DBObjs struct {
	Schemas         map[semantics.Ident]SchemaObjs
	MatchAllSchemas bool
	MatchAllObjects bool
}

func newDBObjs(db semantics.Ident, o DBObjs, om semantics.ObjMatcher) DBObjs {
	r := DBObjs{Schemas: map[semantics.Ident]SchemaObjs{}}
	r = r.setMatchAllSchemas(db, om)
	r.setMatchAllObjects(db, om)
	for schema, schemaObjs := range o.Schemas {
		if !om.DisjointFromSchema(db, schema) {
			r.Schemas[schema] = newSchemaObjs(db, schema, schemaObjs, om)
		}
	}
	return r
}

func newDBObjsFromMatched(m *matchedDBObjs) DBObjs {
	o := DBObjs{Schemas: map[semantics.Ident]SchemaObjs{}}
	for k, v := range m.getSchemas() {
		o.Schemas[k] = newSchemaObjsFromMatched(v)
	}
	return o
}

func (o DBObjs) setMatchAllSchemas(db semantics.Ident, om semantics.ObjMatcher) DBObjs {
	if !om.Include[semantics.Schema].MatchAll() {
		return o
	}
	o.MatchAllSchemas = true
	for excludeExpr := range om.Exclude {
		if excludeExpr.MatchesAllObjectsInAnySchemaInDB(db) {
			o.MatchAllSchemas = false
		}
	}
	return o
}

func (o DBObjs) setMatchAllObjects(db semantics.Ident, om semantics.ObjMatcher) DBObjs {
	if om.SupersetOfDB(db) {
		o.MatchAllObjects = true
	}
	return o
}

func (o DBObjs) countByObjType(t ObjType) int {
	r := 0
	for _, schema := range o.Schemas {
		r += schema.countByObjType(t)
	}
	return r
}

func (lhs DBObjs) add(rhs DBObjs) DBObjs {
	// NB this method will alter referenced maps
	if lhs.Schemas == nil {
		return rhs
	}
	for k, v := range rhs.Schemas {
		lhs.Schemas[k] = lhs.Schemas[k].add(v)
	}
	lhs.MatchAllSchemas = lhs.MatchAllSchemas || rhs.MatchAllSchemas
	lhs.MatchAllObjects = lhs.MatchAllObjects || rhs.MatchAllObjects
	return lhs
}
