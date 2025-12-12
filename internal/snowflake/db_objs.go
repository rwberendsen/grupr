package snowflake

import (
	"github.com/rwberendsen/grupr/internal/semantics"
)

type DBObjs struct {
	Schemas  map[string]*SchemaObjs
	MatchAllSchemas bool
	MatchAllObjects bool
}

func newDBObjsFromMatched(m *matchedDBObjs) *DBObjs {
	o := &DBObjs{Schemas: map[string]*SchemaObjs{},}
	for k, v := range m.schemas {
		if !m.hasSchema(k) { continue }
		o.Schemas[k] = newSchemaObjsFromMatched(v)
	}
	return o
}

func newDBObjs(db DBKey, o *DBObjs, e semantics.ObjExpr, om semantics.ObjMatcher) *DBObjs {
	r := &DBObjs{Schemas: map[string]*SchemaObjs{},}
	r.setMatchAllSchemas(db, e, om)
	r.setMatchAllObjects(db, e, om)
	for schema, schemaObjs := range o.Schemas {
		if !e[semantics.Schema].Match(schema) { continue }
		for excludeExpr := range om.Exclude {
			if excludeExpr.MatchesAllObjectsInSchema(db.Name, schema) {
				continue
			}
		}
		o.Schemas[schema] = newSchemaObjs(db, schema, schemaObjs, e, om)
	}
	// WIP: immediately query grants for database roles here? In that case, we
	//      need error handling here, too, which might trigger another product refresh
	//	again
	// Or: do it later, and even store this info in yet another AccountObjs-like tree structure?
}

func (o *DBObjs) setMatchAllSchemas(db DBKey, e semantics.ObjExpr, om semantics.ObjMatcher) {
	if !e[semantics.Schema].MatchAll() { return }
	o.MatchAllSchemas = true
	for excludeExpr := range om.Exclude {
		if excludeExpr.MatchesAllObjectsInAnySchemaInDB(db.Name) {
			o.MatchAllSchemas = false
		}
	}
}

func (o *DBObjs) setMatchAllObjects(db DBKey, e semantics.ObjExpr, om semantics.ObjMatcher) {
	if !o.MatchAllSchemas { return }
	if !e[semantics.Object].MatchAll() { return }
	o.MatchAllObjects = true
	for excludeExpr := range om.Exclude {
		if excludeExpr[semantics.Database].Match(db.Name) {
			o.MatchAllObjects = false
		}
	}
}
