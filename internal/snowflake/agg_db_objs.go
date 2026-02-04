package snowflake

import (
	"context"
	"database/sql"

	"github.com/rwberendsen/grupr/internal/semantics"
	"github.com/rwberendsen/grupr/internal/syntax"
)

type AggDBObjs struct {
	Schemas         map[string]AggSchemaObjs
	MatchAllSchemas bool
	MatchAllObjects bool

	// set when (future) grants are set
	dbRole                        DatabaseRole
	isDBRoleNew                   bool // if true, then no need to query grants
	revokeFutureGrantsToRead      []FutureGrant
	revokeGrantsToRead            []Grant
	isUsageGrantedToRead          bool
	isUsageGrantedToFutureSchemas bool
	isDBRoleGrantedToProductRead  bool
	// has the database role corresponding to this AggDBObjs been granted to the consuming ProductDTAPs already?
	consumedByGranted map[semantics.ProductDTAPID]bool

	// Small lookup table, first index rows, second index columns
	//   		0: PrvSelect	1: PrvRefernces
	// 0: ObjTable
	// 1: ObjView
	//
	isPrivilegeGrantedToFutureObject [2][2]bool
}

func newAggDBObjs(o DBObjs) AggDBObjs {
	r := AggDBObjs{
		Schemas:         make(map[string]AggSchemaObjs, len(o.Schemas)),
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

// Specifically for AggDBObjs.isPrivilegeGrantedToFutureObject
func (_ AggDBObjs) getObjTypeIdx(ot ObjType) int {
	switch ot {
	case ObjTpTable:
		return 0
	case ObjView:
		return 1
	default:
		panic("object type not implemented")
	}
}

// Specifically for AggDBObjs.isPrivilegeGrantedToFutureObject
func (_ AggDBObjs) getPrivilegeIdx(ot ObjType) int {
	switch ot {
	case PrvSelect:
		return 0
	case PrvReferences:
		return 1
	default:
		panic("object type not implemented")
	}
}

func (o AggDBObjs) setFutureGrantTo(_ Mode, grantedOn ObjType, p Privilege) AggDBObjs {
	switch grantedOn {
	case ObjTpSchema:
		switch p {
		case PrvUsage:
			o.isUsageGrantedToFutureSchemas = true
		default:
			panic("unsupported privilege on schema")
		}
	case ObjTpTable, ObjTpView:
		switch p {
		case PrvSelect, PrvReferences:
			o.isPrivilegeGrantedToFutureObject[o.getObjTypeIdx(grantedOn)][o.getPrivilegeIdx(p)] = true
		}
	}
	return o
}

func (o AggDBObjs) hasFutureGrantTo(m Mode, grantedOn ObjType, p Privilege) bool {
	switch grantedOn {
	case ObjTpSchema:
		switch p {
		case PrvUsage:
			return o.isUsageGrantedToFutureSchemas
		}
	case ObjTpTable, ObjTpView:
		switch p {
		case PrvSelect, PrvReferences:
			return o.isPrivilegeGrantedToFutureObject[o.getObjTypeIdx(grantedOn)][o.getPrivilegeIdx(p)]
		}
	}
	return false
}

func (o AggDBObjs) setRevokeFutureGrantTo(m Mode, g FutureGrant) AggDBObjs {
	if m != ModeRead {
		panic("not implemented")
	}
	o.revokeFutureGrantsToRead = append(o.revokeFutureGrantsToRead, g)
	return o
}

func (o AggDBObjs) setGrantTo(m Mode, p Privilege) AggDBObjs {
	if m != ModeRead || p != PrvUsage {
		panic("not implemented")
	}
	o.isUsageGrantedToRead = true
	return o
}

func (o AggDBObjs) hasGrantTo(m Mode, p Privilege) bool {
	return m == ModeRead && p == PrvUsage && o.isUsageGrantedToRead
}

func (o AggDBObjs) setRevokeGrantTo(m Mode, g Grant) AggDBObjs {
	if m != ModeRead {
		panic("not implemented")
	}
	o.revokeGrantsToRead = append(o.revokeGrantsToRead, g)
	return o
}

func (o AggDBObjs) setDatabaseRole(ctx context.Context, synCnf *syntax.Config, cnf *Config, conn *sql.DB, pID string, dtap string, iID string,
	db string, createDBRoleGrants map[string]struct{}, databaseRoles map[DatabaseRole]struct{}) (AggDBObjs, error) {
	o.dBRole = NewDatabaseRole(synCnf, conf, pID, dtap, iID, ModeRead, db)
	if _, ok := databaseRoles[o.dBRole]; !ok {
		if _, ok = createDBRoleGrants[db]; !ok {
			if err := GrantCreateDatabaseRoleToSelf(ctx, cnf, conn, db); err != nil {
				return o, err
			}
		}
		if err := o.dBRole.Create(ctx, cnf, conn); err != nil {
			return o, err
		}
		o.isDBRoleNew = true
	}
}

func (o AggDBObjs) setFutureGrants(ctx context.Context, synCnf *syntax.Config, cnf *Config, conn *sql.DB, pID string, dtap string, iID string,
	db string, oms semantics.ObjMatchers, createDBRoleGrants map[string]struct{}, databaseRoles map[DatabaseRole]struct{}) (AggDBObjs, error) {
	o, err = o.setDatabaseRole(ctx, synCnf, cnf, conn, pID, dtap, iID, db, createDBRoleGrants, databaseRoles)
	if err != nil {
		return o, err
	}
	o.revokeFutureGrantsToRead = []FutureGrant{}
	if !o.isDBRoleNew {
		for g, err := range QueryFutureGrantsToDBRoleFiltered(ctx, conn, db, o.dBRole.Name, cnf.DatabaseRolePrivileges[ModeRead], nil) {
			if err != nil {
				return o, err
			}

			if g.Database != db {
				// This grant should not be granted to this particular database role
				o = o.setRevokeGrantTo(ModeRead, g)
				continue
			}

			switch g.GrantedIn {
			case ObjTpDatabase:
				switch g.GrantedOn {
				case ObjTpSchema:
					if o.MatchAllSchemas {
						o = o.setFutureGrantTo(ModeRead, g.GrantedOn, g.Privilege)
					} else {
						o = o.setRevokeFutureGrantTo(ModeRead, g)
					}
				case ObjTpTable, ObjTpView:
					if o.MatchAllObjects {
						o = o.setFutureGrantTo(ModeRead, g.GrantedOn, g.Privilege)
					} else {
						o = o.setRevokeFutureGrantTo(ModeRead, g)
					}
				default:
					panic("unsupported granted_on object type in future grant")
				}
			case ObjTpSchema:
				if o.hasSchema(g.Schema) {
					if o.Schemas[g.Schema].MatchAllObjects {
						o.Schemas[g.Schema] = o.Schemas[g.Schema].setFutureGrantTo(ModeRead, g.GrantedOn, g.Privilege)
					} else {
						o = o.setRevokeFutureGrantTo(ModeRead, g)
					}
				} else {
					// TODO: A rare oddity. A schema was added after we loaded account objects,
					// and future grants were granted in it to our database role, no less.
					// We could now check if indeed in that schema our expressions would
					// match all or not, e.g., my_db.my_schema.*, or my_db.my*.* would be
					// expressions that match all objects in my_db.my_schema; if there is
					// any such om in oms, we could decide the leave the future grant alive
					// But for now, we'll add it to our grants to revoke
					o = o.setRevokeFutureGrantTo(ModeRead, g)
				}
			default:
				panic("unsupported granted_in object type in future grant")
			}
		}
	}
	return o
}

func (o AggDBObjs) setGrants(ctx context.Context, synCnf *syntax.Config, cnf *Config, conn *sql.DB, db string, oms semantics.ObjMatchers) (AggDBObjs, error) {
	o.revokeGrantsToRead = []Grant{}
	if !o.isDBRoleNew {
		for g, err := range QueryGrantsToDBRoleFiltered(ctx, cnf, conn, db, o.dBRole.Name, cnf.DatabaseRolePrivileges[ModeRead], nil) {
			if err != nil {
				return o, err
			}

			if g.Database != db {
				// This grant should not be granted to this particular database role
				o = o.setRevokeGrantTo(ModeRead, g)
				continue
			}

			switch g.GrantedOn {
			case ObjTpDatabase:
				o = o.setGrantTo(ModeRead, g.Privilege)
			case ObjTpSchema:
				if o.hasSchema(g.Schema) {
					o.Schemas[g.Schema] = o.Schemas[g.Schema].setGrantTo(ModeRead, g.Privilege)
				} else if oms.DisjointFromSchema(g.Database, g.Schema) {
					o = o.setRevokeGrantTo(ModeRead, g)
				} // Ignore this grant, it is correct, even if we did not know about the object's existence yet (result of FUTURE grant, probably)
			case ObjTpTable, ObjTpView:
				if o.hasObject(g.Schema, g.Object) {
					if o.Schemas[g.Schema].Objects[g.Object].ObjectType != g.GrantedOn {
						// A table may have been dropped and a view with the same name created or vice versa
						// A good reason to refresh the product
						return o, ErrObjectNotExistOrAuthorized
					}
					o.Schemas[g.Schema].Objects[g.Object] = o.Schemas[g.Schema].Objects[g.Object].setGrantTo(ModeRead, g.Privilege)
				} else if oms.DisjointFromObject(g.Database, g.Schema, g.Object) {
					o = o.setRevokeGrantTo(ModeRead, g)
				} // Ignore this grant, it is correct, even if we did not know about the object's existence yet (result of FUTURE grant, probably)
			default:
				panic("unsupported granted_on object_type in grant")
			}
		}
	}
}

func (o AggDBObjs) setConsumedByGranted(pdID semantics.ProductDTAPID) AggDBObjs {
	if o.consumedByGranted == nil {
		o.consumedByGranted = map[semantics.ProductDTAPID]bool{}
	}
	o.consumedByGranted[pdID] = true
	return o
}

func (o AggDBObjs) pushToDoFutureGrants(yield func(FutureGrant) bool) bool {
	if o.MatchAllSchemas {
		if !o.hasFutureGrantTo(ModeRead, ObjTpSchema, PrvUsage) {
			if !yield(FutureGrant{
				Privileges:        []PrivilegeComplete{PrivilegeComplete{Privilege: PrvUsage}},
				GrantedOn:         ObjTpSchema,
				GrantedIn:         ObjTpDatabase,
				Database:          o.dbRole.Database,
				GrantedTo:         ObjTpDatabaseRole,
				GrantedToDatabase: o.dbRole.Database,
				GrantedToRole:     o.dbRole.Name,
			}) {
				return false
			}
		}
	}
	if o.MatchAllObjects {
		for _, ot := range [2]ObjType{ObjTpTable, ObjTpView} {
			prvs := []PrivilegeComplete{}
			for _, p := range [2]Privilege{PrvSelect, PrvReferences} {
				if !o.hasFutureGrantTo(ModeRead, ot, p) {
					prvs = append(prvs, PrivilegeComplete{Privilege: p})
				}
			}
			if len(prvs) > 0 {
				if !yield(FutureGrant{
					Privileges:        prvs,
					GrantedOn:         ot,
					GrantedIn:         ObjTpDatabase,
					Database:          o.dbRole.Database,
					GrantedTo:         ObjTpDatabaseRole,
					GrantedToDatabase: o.dbRole.Database,
					GrantedToRole:     o.dbRole.Name,
				}) {
					return false
				}
			}
		}
	}
	for schema, schemaObjs := range o.Schemas {
		if !schemaObjs.pushToDoFutureGrants(yield, o.dbRole, schema) {
			return false
		}
	}
	return true
}

func (o AggDBObjs) pushToDoGrants(yield func(Grant) bool) bool {
	if !o.hasGrantTo(ModeRead, PrvUsage) {
		if !yield(Grant{
			Privileges:        []PrivilegeComplete{PrivilegeComplete{Privilege: PrvUsage}},
			GrantedOn:         ObjTpDatabase,
			Database:          o.dbRole.Database,
			GrantedTo:         ObjTpDatabaseRole,
			GrantedToDatabase: o.dbRole.Database,
			GrantedToRole:     o.dbRole.Name,
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

func (o AggDBObjs) pushToDoFutureRevokes(yield func(FutureGrant) bool) bool {
	for _, g := range o.revokeFutureGrantsToRead {
		if !yield(g) {
			return false
		}
	}
}

func (o AggDBObjs) pushToDoRevokes(yield func(Grant) bool) bool {
	for _, g := range o.revokeGrantsToRead {
		if !yield(g) {
			return false
		}
	}
}
