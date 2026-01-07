package snowflake

import (
	"github.com/rwberendsen/grupr/internal/semantics"
)

type DBObjs struct {
	Schemas  map[string]*SchemaObjs
	MatchAllSchemas bool
	MatchAllObjects bool
	RevokeGrantsTo []GrantToRecord
}

func newDBObjs(db string, o *DBObjs, om semantics.ObjMatcher) *DBObjs {
	r := &DBObjs{Schemas: map[string]*SchemaObjs{},}
	r.setMatchAllSchemas(db, om)
	r.setMatchAllObjects(db, om)
	for schema, schemaObjs := range o.Schemas {
		if !om.DisjointFromSchema(db.Name, schema) {
			r.Schemas[schema] = newSchemaObjs(db, schema, schemaObjs, om)
		}
	}
	return r
}

func newDBObjsFromMatched(m *matchedDBObjs) *DBObjs {
	o := &DBObjs{Schemas: map[string]*SchemaObjs{},}
	for k, v := range m.getSchemas() {
		o.Schemas[k] = newSchemaObjsFromMatched(v)
	}
	return o
}

func (o *DBObjs) setMatchAllSchemas(db string, om semantics.ObjMatcher) {
	if !om.Include[semantics.Schema].MatchAll() { return }
	o.MatchAllSchemas = true
	for excludeExpr := range om.Exclude {
		if excludeExpr.MatchesAllObjectsInAnySchemaInDB(db.Name) {
			o.MatchAllSchemas = false
		}
	}
}

func (o *DBObjs) setMatchAllObjects(db string, om semantics.ObjMatcher) {
	if om.SupersetOf(db.Name) {
		o.MatchAllObjects = true
	}
}

func (o *DBObjs) setGrantsOn/To/Of() {
	return
}
