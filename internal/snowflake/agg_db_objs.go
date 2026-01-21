package snowflake

import (
	"github.com/rwberendsen/grupr/internal/semantics"
)

type AggDBObjs struct {
	Schemas  map[string]AggSchemaObjs
	MatchAllSchemas 	bool
	MatchAllObjects 	bool
	RevokeGrantsTo 		map[Mode]map[GrantToRecord]struct{}
	GrantsTo		map[Mode]map[Privilege]struct{}
	DBRole			DatabaseRole
}

func newAggDBObjs(o DBObjs) AggDBObjs {
	r := AggDBObjs{
		Schemas: map[string]AggSchemaObjs{},
		MatchAllSchemas: o.MatchAllSchemas,
		MatchAllObjects: o.MatchAllObjects,
	}
	for schema, schemaObjs := range o.Schemas {
		r.Schemas[schema] = newAggSchemaObjs(schemaObjs)
	}
	return r
}

func (o AggDBObjs) hasSchema(s string) bool {
	return o.Schemas[s] != nil
}

func (o AggDBObjs) hasObject(s string, obj string) bool {
	return o.hasSchema(s) && o.Schemas[s].hasObject(obj)
}

func (o DBObjs) setGrantTo(m Mode, p Privilege) DBObjs {
	if o.GrantsTo == nil { o.GrantsTo = map[Mode]map[Privilege]struct{}{} }
	if _, ok := o.GrantsTo[m]; !ok { o.GrantsTo[m] = map[Privilege]struct{}{} }
	o.GrantsTo[m][p] = struct{}{}
	return o
}

func (o DBObjs) hasGrantTo(m Mode, p Privilege) {
	if v, ok := o.GrantsTo[m] {
		_, ok = v[p]
		return ok
	}
}

func (o DBObjs) setRevokeGrantTo(m Mode, g GrantToRole) DBObjs {
	if o.RevokeGrantsTo == nil { o.RevokeGrantsTo = map[Mode]map[GrantToRole]struct{}{} }
	if _, ok := o.RevokeGrantsTo[m]; !ok { o.RevokeGrantsTo[m] = map[GrantToRole]struct{}{} }
	o.RevokeGrantsTo[m][g] = struct{}{}
	return o
}

func (o DBObjs) grant(ctx context.Context, synCnf *syntax.Config, cnf *Config, conn *sql.DB, pID string, dtap string, iID string,
		db string, om semantics.ObjMatcher, createDBRoleGrants map[string]struct{}, databaseRoles map[DatabaseRole]struct{}) (DBObjs, error) {
	o.DBRole = NewDatabaseRole(synCnf, conf, pID, dtap, iID, ModeRead, db)
	if _, ok := databaseRoles[db][o.DBRole]; !ok {
		if _, ok = createDBRoleGrants[db]; !ok {
			if err := GrantCreateDatabaseRoleToSelf(ctx, cnf, conn, db); err != nil { return o, err }
		}
		if err := o.DBRole.Create(ctx, cnf, conn); err != nil { return o, err }
	} else {
		// TODO: FUTURE GRANTS (to be granted first (and revoked first, too, when it comes to revoking)
		// 	Together with FUTURE grants, we can always execute an complementary ALL grant as well, immediately after.
		// TODO: if o.MatchAllObjects, then no need to query grants, we'll just GRANT ALL anyway!?
		for g, err := range QueryGrantsToDBRoleFiltered(ctx, conn, db, o.DBRole.Name, cnf.DatabaseRolePrivileges[ModeRead], nil) {
			if err != nil { return o, err }

			if g.Database != db {
				// This grant should not be granted to this particular database role
				o = o.setRevokeGrantTo(ModeRead, g)
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
	if err := o.doGrant(ctx, cnf, conn); err != nil { return o, err }
	return o, nil
	// TODO: SHOW GRANTS ON / OF database role, and store them in DBObjs / process them
}

func (o *DBObjs) doGrant(ctx context.Context, cnf *Config, conn *sql.DB) error {
	if !o.hasGrantTo(ModeRead, PrvUsage) {
		if err := GrantToRole{
				Privilege: PrvUsage,
				GrantedOn: ObjTpDatabase,
				Database: o.DBRole.Database,
		}.DoGrantToDBRole(ctx, cnf, conn, o.DBRole.Database, o.DBRole.Name); err != nil {
			return err
		}
	}
	for schema, schemaObjs := range o.Schemas {
		if err := schemaObjs.doGrant(ctx, cnf, conn, o.DBRole.Database, schema, o.DBRole.Name); err != nil {
			return err
		}
	}
	return nil
}
