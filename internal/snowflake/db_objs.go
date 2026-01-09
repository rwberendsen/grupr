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

func (o *DBObjs) grant(ctx context.Context, synCnf *syntax.Config, cnf *Config, conn *sql.DB, pID string, dtap string, iID string,
		db string, createDBRoleGrants map[string]struct{}, databaseRoles map[string]map[DatabaseRole]struct{}) {
	dbRole := NewDatabaseRole(synCnf, conf, pID, dtap, iID, ModeRead, db)
	if _, ok := databaseRoles[db][dbRole]; !ok {
		if _, ok = createDBRoleGrants[db]; !ok {
			if err := GrantCreateDatabaseRoleToSelf(ctx, cnf, conn, db); err != nil { return err }
		}
		if err := dbRole.Create(ctx, cnf, conn); err != nil { return err }
	} else {
		for grant, err := range QueryGrantsToDBRoleFiltered(ctx, conn, db, dbRole.Name,
				map[Privilege]struct{}{
					PrvUsage: {},
					PrvSelect: {},
					PrvReferences: {},
				},
				nil) {
			// do something WIP
		}
	}
	return
			// SHOW GRANTS TO / ON / OF database role, and store them in DBObjs
			// grants on objects should be stored on the respective accountobjects
			// if no accountobjects is there, it means this is a grant that should be revoked, later,
			// and it should be stored separately, for later processing, after all grants have been done.
}
