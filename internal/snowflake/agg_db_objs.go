package snowflake

import (
	"context"
	"database/sql"

	"github.com/rwberendsen/grupr/internal/semantics"
	"github.com/rwberendsen/grupr/internal/syntax"
)

type AggDBObjs struct {
	Schemas         map[semantics.Ident]AggSchemaObjs
	MatchAllSchemas bool
	MatchAllObjects bool

	// set when (future) grants are set
	dbRole                           DatabaseRole
	isDBRoleNew                      bool // if true, then no need to query grants
	revokeFutureGrantsToReadRole     []FutureGrant
	revokeGrantsToReadRole           []Grant
	isUsageGrantedToReadRole         bool
	isUsageGrantedToFutureSchemas    bool
	isDBRoleGrantedToProductReadRole bool
	// has the database role corresponding to this AggDBObjs been granted to the consuming ProductDTAPs already?
	consumedByGranted map[semantics.ProductDTAPID]bool

	// Small lookup table, first index rows, second index columns
	//   		0: PrvSelect	1: PrvRefernces
	// 0: ObjTable
	// 1: ObjView
	//
	isPrivilegeOnFutureObjectGrantedToReadRole             [2][2]bool
	isCreateObjectOnFutureSchemasGrantedToProductWriteRole [2]bool // 0: ObjTable, 1: ObjView
}

func newAggDBObjs(o DBObjs) AggDBObjs {
	r := AggDBObjs{
		Schemas:         make(map[semantics.Ident]AggSchemaObjs, len(o.Schemas)),
		MatchAllSchemas: o.MatchAllSchemas,
		MatchAllObjects: o.MatchAllObjects,
	}
	for schema, schemaObjs := range o.Schemas {
		r.Schemas[schema] = newAggSchemaObjs(schemaObjs)
	}
	return r
}

func (o AggDBObjs) hasSchema(s semantics.Ident) bool {
	_, ok := o.Schemas[s]
	return ok
}

func (o AggDBObjs) hasObject(s semantics.Ident, obj semantics.Ident) bool {
	return o.hasSchema(s) && o.Schemas[s].hasObject(obj)
}

// Specifically for AggDBObjs.isPrivilegeOnFutureObjectGrantedToReadRole
func (_ AggDBObjs) getPrivilegeIdx(p Privilege) int {
	switch p {
	case PrvSelect:
		return 0
	case PrvReferences:
		return 1
	default:
		panic("privilege not implemented")
	}
}

func (o AggDBObjs) setFutureGrantTo(m Mode, g FutureGrant) AggDBObjs {
	switch m {
	case ModeRead:
		switch g.GrantedOn {
		case ObjTpSchema:
			switch g.Privileges[0].Privilege {
			case PrvUsage:
				o.isUsageGrantedToFutureSchemas = true
			}
			// Ignore; unmanaged grant
		case ObjTpTable, ObjTpView:
			switch g.Privileges[0].Privilege {
			case PrvSelect, PrvReferences:
				o.isPrivilegeOnFutureObjectGrantedToReadRole[g.GrantedOn.getIdxObjectLevel()][g.Privileges[0].Privilege.getIdxObjectLevel()] = true
			}
			// Ignore; unmanaged grant
		}
	case ModeWrite:
		switch g.GrantedOn {
		case ObjTpSchema:
			switch g.Privileges[0].Privilege {
			case PrvCreate:
				o.isCreateObjectOnFutureSchemasGrantedToProductWriteRole[g.Privileges[0].CreateObjectType.getIdxObjectLevel()] = true
			}
			// Ignore, unmanaged grant
		}
		// Ignore, unmanaged grant
	}
	return o
}

func (o AggDBObjs) hasFutureGrantTo(m Mode, grantedOn ObjType, p PrivilegeComplete) bool {
	switch m {
	case ModeRead:
		switch grantedOn {
		case ObjTpSchema:
			switch p.Privilege {
			case PrvUsage:
				return o.isUsageGrantedToFutureSchemas
			}
		case ObjTpTable, ObjTpView:
			switch p.Privilege {
			case PrvSelect, PrvReferences:
				return o.isPrivilegeOnFutureObjectGrantedToReadRole[grantedOn.getIdxObjectLevel()][p.getIdxObjectLevel()]
			}
		}
	case ModeWrite:
		switch grantedOn {
		case ObjTpSchema:
			switch p.Privilege {
			case PrvCreate:
				return o.isCreateObjectOnFutureSchemasGrantedToProductWriteRole[grantedOn.getIdxObjectLevel()]
			}
		}
	}
	return false
}

func (o AggDBObjs) setRevokeFutureGrantTo(m Mode, g FutureGrant) AggDBObjs {
	if m != ModeRead {
		panic("not implemented")
	}
	o.revokeFutureGrantsToReadRole = append(o.revokeFutureGrantsToReadRole, g)
	return o
}

func (o AggDBObjs) setGrantTo(m Mode, g Grant) AggDBObjs {
	if m == ModeRead && g.Privileges[0].Privilege == PrvUsage {
		o.isUsageGrantedToReadRole = true
	}
	// Ignore; unmanaged grant
	return o
}

func (o AggDBObjs) hasGrantTo(m Mode, p Privilege) bool {
	return m == ModeRead && p == PrvUsage && o.isUsageGrantedToReadRole
}

func (o AggDBObjs) setRevokeGrantTo(m Mode, g Grant) AggDBObjs {
	if m != ModeRead {
		panic("not implemented")
	}
	o.revokeGrantsToReadRole = append(o.revokeGrantsToReadRole, g)
	return o
}

func (o AggDBObjs) setDatabaseRole(ctx context.Context, synCnf *syntax.Config, cnf *Config, conn *sql.DB, pID string, dtap string, iID string,
	db semantics.Ident, databaseRoles map[DatabaseRole]struct{}) (AggDBObjs, error) {
	o.dbRole = NewDatabaseRole(synCnf, cnf, pID, dtap, iID, ModeRead, db)
	if _, ok := databaseRoles[o.dbRole]; !ok {
		if err := o.dbRole.Create(ctx, cnf, conn); err != nil {
			return o, err
		}
		o.isDBRoleNew = true
	}
	return o, nil
}

func (o AggDBObjs) setFutureGrants(ctx context.Context, synCnf *syntax.Config, cnf *Config, conn *sql.DB, pID string, dtap string, iID string,
	db semantics.Ident, oms semantics.ObjMatchers, databaseRoles map[DatabaseRole]struct{}) (AggDBObjs, error) {
	o, err := o.setDatabaseRole(ctx, synCnf, cnf, conn, pID, dtap, iID, db, databaseRoles)
	if err != nil {
		return o, err
	}
	o.revokeFutureGrantsToReadRole = []FutureGrant{}
	if !o.isDBRoleNew {
		for g, err := range QueryFutureGrantsToDBRoleFiltered(ctx, conn, db, o.dbRole.Name, cnf.DatabaseRolePrivileges[ModeRead], nil) {
			if err != nil {
				return o, err
			}

			if g.Database != db {
				// This grant should not be granted to this particular database role
				o = o.setRevokeFutureGrantTo(ModeRead, g)
				continue
			}

			switch g.GrantedIn {
			case ObjTpDatabase:
				switch g.GrantedOn {
				case ObjTpSchema:
					if o.MatchAllSchemas {
						o = o.setFutureGrantTo(ModeRead, g)
					} else {
						o = o.setRevokeFutureGrantTo(ModeRead, g)
					}
				case ObjTpTable, ObjTpView:
					if o.MatchAllObjects {
						o = o.setFutureGrantTo(ModeRead, g)
					} else {
						o = o.setRevokeFutureGrantTo(ModeRead, g)
					}
				}
				// Ignore this grant, it's not in grupr its scope (unmanaged grant)
			case ObjTpSchema:
				if o.hasSchema(g.Schema) {
					if o.Schemas[g.Schema].MatchAllObjects {
						o.Schemas[g.Schema] = o.Schemas[g.Schema].setFutureGrantTo(ModeRead, g)
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
	return o, nil
}

func (o AggDBObjs) setFutureGrantsToWriteRole(ctx context.Context, synCnf *syntax.Config, cnf *Config, conn *sql.DB, pID string, dtap string,
	db semantics.Ident, oms semantics.ObjMatchers) (AggDBObjs, error) {
	o.revokeFutureGrantsToWrite = []FutureGrant{}
	for g, err := range QueryFutureGrantsToDBRoleFiltered(ctx, conn, db, o.dbRole.Name, cnf.DatabaseRolePrivileges[ModeRead], nil) {
		if err != nil {
			return o, err
		}

		if g.Database != db {
			// This grant should not be granted to this particular database role
			o = o.setRevokeFutureGrantTo(ModeRead, g)
			continue
		}

		switch g.GrantedIn {
		case ObjTpDatabase:
			switch g.GrantedOn {
			case ObjTpSchema:
				if o.MatchAllSchemas {
					o = o.setFutureGrantTo(ModeRead, g)
				} else {
					o = o.setRevokeFutureGrantTo(ModeRead, g)
				}
			case ObjTpTable, ObjTpView:
				if o.MatchAllObjects {
					o = o.setFutureGrantTo(ModeRead, g)
				} else {
					o = o.setRevokeFutureGrantTo(ModeRead, g)
				}
			}
			// Ignore this grant, it's not in grupr its scope (unmanaged grant)
		case ObjTpSchema:
			if o.hasSchema(g.Schema) {
				if o.Schemas[g.Schema].MatchAllObjects {
					o.Schemas[g.Schema] = o.Schemas[g.Schema].setFutureGrantTo(ModeRead, g)
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
	return o, nil
}

func (o AggDBObjs) setGrants(ctx context.Context, synCnf *syntax.Config, cnf *Config, conn *sql.DB, db semantics.Ident, oms semantics.ObjMatchers) (AggDBObjs, error) {
	o.revokeGrantsToReadRole = []Grant{}
	if !o.isDBRoleNew {
		for g, err := range QueryGrantsToDBRoleFiltered(ctx, cnf, conn, db, o.dbRole.Name, true, cnf.DatabaseRolePrivileges[ModeRead], nil) {
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
				o = o.setGrantTo(ModeRead, g)
			case ObjTpSchema:
				if o.hasSchema(g.Schema) {
					o.Schemas[g.Schema] = o.Schemas[g.Schema].setGrantTo(ModeRead, g)
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
					o.Schemas[g.Schema].Objects[g.Object] = o.Schemas[g.Schema].Objects[g.Object].setGrantTo(ModeRead, g)
				} else if oms.DisjointFromObject(g.Database, g.Schema, g.Object) {
					o = o.setRevokeGrantTo(ModeRead, g)
				} // Ignore this grant, it is correct, even if we did not know about the object's existence yet (result of FUTURE grant, probably)
			}
			// Ignore this grant, it is not managed by grupr at the moment, sysadmins may have granted it if it is not in grupr's scope currently
		}
	}
	return o, nil
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
		if !o.hasFutureGrantTo(ModeRead, ObjTpSchema, PrivilegeComplete{Privilege: PrvUsage}) {
			if !yield(FutureGrant{
				Privileges:        []PrivilegeComplete{PrivilegeComplete{Privilege: PrvUsage}},
				GrantedOn:         ObjTpSchema,
				GrantedIn:         ObjTpDatabase,
				Database:          o.dbRole.Database,
				GrantedTo:         ObjTpDatabaseRole,
				GrantedToDatabase: o.dbRole.Database,
				GrantedToName:     o.dbRole.Name,
			}) {
				return false
			}
		}
	}
	if o.MatchAllObjects {
		for _, ot := range [2]ObjType{ObjTpTable, ObjTpView} {
			prvs := []PrivilegeComplete{}
			for _, p := range [2]PrivilegeComplete{PrivilegeComplete{Privilege: PrvSelect}, PrivilegeComplete{Privilege: PrvReferences}} {
				if !o.hasFutureGrantTo(ModeRead, ot, p) {
					prvs = append(prvs, p)
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
					GrantedToName:     o.dbRole.Name,
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
			GrantedToName:     o.dbRole.Name,
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
	for _, g := range o.revokeFutureGrantsToReadRole {
		if !yield(g) {
			return false
		}
	}
	return true
}

func (o AggDBObjs) pushToDoRevokes(yield func(Grant) bool) bool {
	for _, g := range o.revokeGrantsToReadRole {
		if !yield(g) {
			return false
		}
	}
	return true
}
