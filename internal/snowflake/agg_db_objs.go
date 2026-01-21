package snowflake

import (
	"github.com/rwberendsen/grupr/internal/semantics"
)

type AggDBObjs struct {
	Schemas  map[string]AggSchemaObjs
	MatchAllSchemas 	bool
	MatchAllObjects 	bool

	// set when grant() is called
	dBRole			DatabaseRole
	revokeGrantsToRead	map[Grant]struct{}
	isUsageGrantedToRead	bool
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

func (o AggDBObjs) setGrantTo(m Mode, p Privilege) AggDBObjs {
	if m != ModeRead || p != PrvUsage { panic("not implemented") }
	o.isUsageGrantedToRead = true
	return o
}

func (o AggDBObjs) hasGrantTo(m Mode, p Privilege) {
	return m == ModeRead && p == PrvUsage && o.isUsageGrantedToRead
}

func (o AggDBObjs) setRevokeGrantTo(m Mode, g GrantToRole) AggDBObjs {
	if m != ModeRead { panic("not implemented") }
	o.revokeGrantsToRead[g] = struct{}{}
	return o
}

func (o AggDBObjs) grant(ctx context.Context, synCnf *syntax.Config, cnf *Config, conn *sql.DB, pID string, dtap string, iID string,
		db string, om semantics.ObjMatcher, createDBRoleGrants map[string]struct{}, databaseRoles map[DatabaseRole]struct{}) (AggDBObjs, error) {
	o.dBRole = NewDatabaseRole(synCnf, conf, pID, dtap, iID, ModeRead, db)
	o.revokeGrantsToRead = map[Grant]struct{}{}
	if _, ok := databaseRoles[o.dBRole]; !ok {
		if _, ok = createDBRoleGrants[db]; !ok {
			if err := GrantCreateDatabaseRoleToSelf(ctx, cnf, conn, db); err != nil { return o, err }
		}
		if err := o.dBRole.Create(ctx, cnf, conn); err != nil { return o, err }
	} else {
		// TODO: FUTURE GRANTS (to be granted first (and revoked first, too, when it comes to revoking)
		// 	Together with FUTURE grants, we can always execute an complementary ALL grant as well, immediately after.
		// TODO: if o.MatchAllObjects, then no need to query grants, we'll just GRANT ALL anyway!?
		for g, err := range QueryGrantsToDBRoleFiltered(ctx, conn, db, o.dBRole.Name, cnf.DatabaseRolePrivileges[ModeRead], nil) {
			if err != nil { return o, err }

			if g.Database != db {
				// This grant should not be granted to this particular database role
				o = o.setRevokeGrantTo(ModeRead, g)
				continue
			}

			switch {
			case g.GrantedOn == ObjTpDatabase:
				o = o.setGrantTo(ModeRead, g.Privilege)
			case g.GrantedOn == ObjTpSchema:
				if o.hasSchema(g.Schema) {
					o.Schemas[g.Schema] = o.Schemas[g.Schema].setGrantTo(ModeRead, g.Privilege)
				} else if om.DisjointFromSchema(g.Database, g.Schema) {
					o = o.setRevokeGrantTo(ModeRead, g)
				} // Ignore this grant, it is correct, even if we did not know about the object's existence yet (result of FUTURE grant, probably)
			case g.GrantedOn == ObjTpTable || g.GrantedOn == ObjTpView:
				if o.hasObject(g.Schema, g.Object) {
					if o.Schemas[g.Schema].Objects[g.Object].ObjectType != g.GrantedOn {
						// A table may have been dropped and a view with the same name created or vice versa
						// A good reason to refresh the product
						return o, ErrObjectNotExistOrAuthorized 
					}
					o.Schemas[g.Schema].Objects[g.Object] = o.Schemas[g.Schema].Objects[g.Object].setGrantTo(ModeRead, g.Privilege)
				} else if om.DisjointFromObject(g.Database, g.Schema, g.Object) {
					o = o.setRevokeGrantTo(ModeRead, g)
				} // Ignore this grant, it is correct, even if we did not know about the object's existence yet (result of FUTURE grant, probably)
			}
		}
	}
	if err := o.doGrant(ctx, cnf, conn); err != nil { return o, err }
	return o, nil
	// TODO: SHOW GRANTS ON / OF database role, and store them in AggDBObjs / process them
}

func (o AggDBObjs) pushToDoGrants(yield func(Grant) bool) bool {
	if !o.hasGrantTo(ModeRead, PrvUsage) {
		if !yield(Grant{
				Privilege: PrvUsage,
				GrantedOn: ObjTpDatabase,
				Database: o.dbRole.Database,
				GrantedTo: ObjTpDatabaseRole,
				GrantedToDatabase: o.dbRole.Database,
				GrantedToRole: o.dbRole.Name,
		}) {
			return false
		}
	}
	for schema, schemaObjs := range o.Schemas {
		if !schemaObjs.pushToDoGrants(yield, o.dbRole, schema) {
			return false
		}
	}
	return true
}
