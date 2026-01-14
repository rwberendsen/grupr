package snowflake

import (
	"github.com/rwberendsen/grupr/internal/semantics"
)

type DBObjs struct {
	Schemas  map[string]*SchemaObjs
	MatchAllSchemas 	bool
	MatchAllObjects 	bool
	RevokeGrantsTo 		map[Mode]map[GrantToRecord]struct{}
	GrantsTo		map[Mode]map[Privilege]struct{}
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

func (o *DBObjs) hasSchema(s string) bool {
	return o.Schemas[s] != nil
}

func (o *DBObjs) hasObject(s string, obj string) bool {
	return o.hasSchema(s) && o.Schemas[s].hasObject(obj)
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

func (o *DBObjs) setGrantTo(m Mode, p Privilege) {
	if o.GrantsTo == nil { o.GrantsTo = map[Mode]map[Privilege]struct{}{} }
	if _, ok := o.GrantsTo[m]; !ok { o.GrantsTo[m] = map[Privilege]struct{}{} }
	o.GrantsTo[m][p] = struct{}{}
}

func (o *DBObjs) setRevokeGrantTo(m Mode, g GrantToRole) {
	if o.RevokeGrantsTo == nil { o.RevokeGrantsTo = map[Mode]map[GrantToRole]struct{}{} }
	if _, ok := o.RevokeGrantsTo[m]; !ok { o.RevokeGrantsTo[m] = map[GrantToRole]struct{}{} }
	o.RevokeGrantsTo[m][g] = struct{}{}
}

func (o *DBObjs) grant(ctx context.Context, synCnf *syntax.Config, cnf *Config, conn *sql.DB, pID string, dtap string, iID string,
		db string, om semantics.ObjMatcher, createDBRoleGrants map[string]struct{}, databaseRoles map[DatabaseRole]struct{}) error {
	dbRole := NewDatabaseRole(synCnf, conf, pID, dtap, iID, ModeRead, db)
	if _, ok := databaseRoles[db][dbRole]; !ok {
		if _, ok = createDBRoleGrants[db]; !ok {
			if err := GrantCreateDatabaseRoleToSelf(ctx, cnf, conn, db); err != nil { return err }
		}
		if err := dbRole.Create(ctx, cnf, conn); err != nil { return err }
	} else {
		for g, err := range QueryGrantsToDBRoleFiltered(ctx, conn, db, dbRole.Name, cnf.DatabaseRolePrivileges[ModeRead], nil) {
			if err != nil { return err }

			if g.Database != db {
				// This grant should not be granted to this particular database role
				o.setRevokeGrantTo(ModeRead, g)
				continue
			}

			switch {
			case g.GrantedOn == ObjTpDatabase:
				o.SetGrantTo(ModeRead, g.Privilege)
			case g.GrantedOn == ObjTpSchema:
				if o.hasSchema(g.Schema) {
					o.Schemas[g.Schema].setGrantTo(ModeRead, g.Privilege)
				} else if om.DisjointFromSchema(g.Database, g.Schema) {
					o.SetRevokeGrantTo(ModeRead, g)
				} // Ignore this grant, it is correct, even if we did not know about the object's existence yet (result of FUTURE grant, probably)
			case g.GrantedOn == ObjTpTable || g.GrantedOn == ObjTpView:
				if o.hasObject(g.Schema, g.Object) {
					if o.Schemas[g.Schema].Objects[g.Object].ObjectType != g.GrantedOn {
						// A table may have been dropped and a view with the same name created or vice versa
						// A good reason to refresh the product
						return ErrObjectNotExistOrAuthorized 
					}
					o.Schemas[g.Schema].Objects[g.Object].setGrantTo(ModeRead, g.Privilege)
				} else if om.DisjointFromObject(g.Database, g.Schema, g.Object) {
					o.SetRevokeGrantTo(ModeRead, g)
				} // Ignore this grant, it is correct, even if we did not know about the object's existence yet (result of FUTURE grant, probably)
			}
		}
	}
	// and now we run over the objects in our DBObjs, and if the necessary privileges have not yet been granted, we grant them
	return
			// SHOW GRANTS TO / ON / OF database role, and store them in DBObjs
}
