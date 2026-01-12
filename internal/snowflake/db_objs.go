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
	o.GrantsTo[m][p] = struct{}
}

func (o *DBObjs) setRevokeGrantTo(m Mode, g GrantToRole) {
	if o.RevokeGrantsTo == nil { o.RevokeGrantsTo = map[Mode]map[GrantToRole]struct{}{} }
	if _, ok := o.RevokeGrantsTo[m]; !ok { o.RevokeGrantsTo[m] = map[GrantToRole]struct{}{} }
	o.RevokeGrantsTo[m][g] = struct{}{}
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
		// TODO: if the database was dropped concurrently, then so was the database role
		// Then grant here may return err == ErrObjectNotExistOrAuthorized
		// Then we could refresh the product, once we reach back to the product level
		// Then, if the database was re-created concurrently, we would come back here eventually,
		// for the same database. And then, according to our records in databaseRoles, the database
		// role should still exist (but in reality it does not). That would cause a loop, where 
		// the product would be refreshed again and again until refreshes are exhausted.
		// So, when we refresh a product, we should also refresh databaseRoles. But that is weird,
		// products are running in separate threads. Therefore, we may have to abandon the global
		// databaseRoles map. Or, make it part of the accountCache perhaps, so that when we
		// refresh a product, the presence or absence of database roles is handled, too.
		for g, err := range QueryGrantsToDBRoleFiltered(ctx, conn, db, dbRole.Name,
				map[GrantToRole]struct{}{
					GrantToRole{
						Privilege: PrvUsage,
						GrantedOn: ObjTpDatabase,
					}: {},
					GrantToRole{
						Privilege: PrvUsage,
						GrantedOn: ObjTpSchema,
					}: {},
					GrantToRole{
						Privilege: PrvSelect,
						GrantedOn: ObjTpTable,
					}: {},
					GrantToRole{
						Privilege: PrvSelect,
						GrantedOn: ObjTpView,
					}: {},
					GrantToRole{
						Privilege: PrvReferences,
						GrantedOn: ObjTpTable,
					}: {},
					GrantToRole{
						Privilege: PrvReferences,
						GrantedOn: ObjTpView,
					}: {},
				},
				nil) {
			if err != nil { return err }
			switch {
			case g.Privilege == PrvUsage && g.GrantedOn == ObjTpDatabase && g.Database == db:
				o.setGrantTo(ModeRead, PrvUsage)
			case g.Privilege == Usage && g.GrantedOn == ObjTpSchema && g.Database == db && o.hasSchema(g.Schema): 
				o.Schemas[g.Schema].setGrantTo(ModeRead, PrvUsage)
			case (g.Privilege == PrvSelect || g.Privilege == References) && (g.GrantedOn == ObjTpTable || g.GrantedOn == ObjTypeView) \
					&& g.Database == db && o.hasSchema(g.Schema) && o.Schemas[g.Schema].hasObject(g.Object, g.GrantedOn):
				o.Schemas[g.Schema].Objects[ObjKey{Name: g.Object, ObjectType: g.GrantedOn}].setGrantTo(ModeRead, g.Privilege)
			default:
				o.setRevokeGrantTo(ModeRead, g)
			}
		}
		// and now we run over the objects in our DBObjs, and if the necessary privileges have not yet been granted, we grant them
	}
	return
			// SHOW GRANTS TO / ON / OF database role, and store them in DBObjs
			// grants on objects should be stored on the respective accountobjects
			// if no accountobjects is there, it means this is a grant that should be revoked, later,
			// and it should be stored separately, for later processing, after all grants have been done.
}
