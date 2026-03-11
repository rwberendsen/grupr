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

	// Set when (future) grants are set
	readDBRole                           DatabaseRole
	isReadDBRoleNew                      bool // if true, then no need to query grants

	// Grants to the readDBRole
	isUsageGrantedToReadDBRole       bool
	isUsageGrantedOnFutureSchemasToReadDBRole    bool
	// Small lookup table, first index rows, second index columns
	//   		0: PrvSelect	1: PrvRefernces
	// 0: ObjTable
	// 1: ObjView
	//
	isPrivilegeOnFutureObjectGrantedToReadDBRole             [2][2]bool
	revokeGrantsToReadDBRole           []Grant
	revokeFutureGrantsToReadDBRole     []FutureGrant

	// Has the readDBRole been granted to the consuming ProductDTAPs already?
	// TODO: can this be a struct{} value type?
	consumedByGranted map[semantics.ProductDTAPID]bool

	// Grants to the product read role; only used if this AggDBObjs is part of a product level interface
	isReadDBRoleGrantedToProductReadRole bool // directly set from within Grupin.setDBRoleGrants

	// Grants to the product write role; only used if this AggDBObjs is part of a product level interface
	revokeFutureGrantsToProductWriteRole    []FutureGrant
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

func (o AggDBObjs) setFutureGrantTo(m Mode, g FutureGrant) AggDBObjs {
	// Used for setting if grants on future objects in AggDBObjs have been
	// granted to either the readDBRole (ModeRead) or the ProductWriteRole
	// (ModeWrite)
	switch m {
	case ModeRead:
		switch g.GrantedOn {
		case ObjTpSchema:
			switch g.Privileges[0].Privilege {
			case PrvUsage:
				o.isUsageGrantedOnFutureSchemasToReadDBRole = true
			}
			// Ignore; unmanaged grant
		case ObjTpTable, ObjTpView:
			switch g.Privileges[0].Privilege {
			case PrvSelect, PrvReferences:
				o.isPrivilegeOnFutureObjectGrantedToReadDBRole[g.GrantedOn.getIdxObjectLevel()][g.Privileges[0].Privilege.getIdxObjectLevel()] = true
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
	// Used for setting if grants on future objects in AggDBObjs have been
	// granted to either the readDBRole (ModeRead) or the ProductWriteRole
	// (ModeWrite)
	switch m {
	case ModeRead:
		switch grantedOn {
		case ObjTpSchema:
			switch p.Privilege {
			case PrvUsage:
				return o.isUsageGrantedOnFutureSchemasToReadDBRole
			}
		case ObjTpTable, ObjTpView:
			switch p.Privilege {
			case PrvSelect, PrvReferences:
				return o.isPrivilegeOnFutureObjectGrantedToReadDBRole[grantedOn.getIdxObjectLevel()][p.Privilege.getIdxObjectLevel()]
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
	// Used only for ModeRead at the moment, but, in the future we will have a ModeOps, most likely
	// Note that ModeWrite privileges to be revoked are stored in ProductDTAP
	if m != ModeRead {
		panic("not implemented")
	}
	o.revokeFutureGrantsToReadDBRole = append(o.revokeFutureGrantsToReadDBRole, g)
	return o
}

func (o AggDBObjs) setGrantTo(m Mode, g Grant) AggDBObjs {
	// Used for setting if grants on objects in AggDBObjs have been
	// granted to either the readDBRole (ModeRead) or the ProductWriteRole
	// (ModeWrite); the latter currently has no direct privileges on a DB
	switch m {
	case ModeRead:
		switch g.Privileges[0].Privilege {
		case PrvUsage:
			o.isUsageGrantedToReadDBRole = true
		}
	}
	return o
}

func (o AggDBObjs) hasGrantTo(m Mode, p Privilege) bool {
	// Used for checking if grants on objects in AggDBObjs have been
	// granted to either the readDBRole (ModeRead) or the ProductWriteRole
	// (ModeWrite); the latter currently has no direct privileges on a DB
	switch m {
	case ModeRead:
		switch p {
		case PrvUsage:
			return o.isUsageGrantedToReadDBRole
		}
	}
	return false
}

func (o AggDBObjs) setRevokeGrantTo(m Mode, g Grant) AggDBObjs {
	// Used only for ModeRead at the moment, but, in the future we will have a ModeOps, most likely
	// Note that ModeWrite privileges to be revoked are stored in ProductDTAP
	if m != ModeRead {
		panic("not implemented")
	}
	o.revokeGrantsToReadDBRole = append(o.revokeGrantsToReadDBRole, g)
	return o
}

func (o AggDBObjs) setDatabaseRole(ctx context.Context, synCnf *syntax.Config, cnf *Config, conn *sql.DB, pID string, dtap string, iID string,
	db semantics.Ident, databaseRoles map[DatabaseRole]struct{}) (AggDBObjs, error) {
	o.readDBRole = NewDatabaseRole(synCnf, cnf, pID, dtap, iID, ModeRead, db)
	if _, ok := databaseRoles[o.readDBRole]; !ok {
		if err := o.readDBRole.Create(ctx, cnf, conn); err != nil {
			return o, err
		}
		o.isReadDBRoleNew = true
	}
	return o, nil
}

func (o AggDBObjs) setFutureGrants(ctx context.Context, synCnf *syntax.Config, cnf *Config, conn *sql.DB, pID string, dtap string, iID string,
	db semantics.Ident, oms semantics.ObjMatchers, databaseRoles map[DatabaseRole]struct{}) (AggDBObjs, error) {
	o, err := o.setDatabaseRole(ctx, synCnf, cnf, conn, pID, dtap, iID, db, databaseRoles)
	if err != nil {
		return o, err
	}
	if !o.isReadDBRoleNew {
		for g, err := range QueryFutureGrantsToDBRoleFiltered(ctx, conn, db, o.readDBRole.Name, cnf.DatabaseRolePrivileges[ModeRead], nil) {
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
					// A rare oddity. A schema was added after we loaded account objects,
					// and future grants were granted in it to our database role, no less.
					// But, if the YAML indicates this is correct, we will leave the grant intact
					if !oms.MatchAllObjectsInSchema(db, g.Schema) {
						o = o.setRevokeFutureGrantTo(ModeRead, g)
					}
				}
			default:
				panic("unsupported granted_in object type in future grant")
			}
		}
	}
	return o, nil
}

func (o AggDBObjs) setGrants(ctx context.Context, synCnf *syntax.Config, cnf *Config, conn *sql.DB, db semantics.Ident, oms semantics.ObjMatchers) (AggDBObjs, error) {
	if !o.isReadDBRoleNew {
		for g, err := range QueryGrantsToDBRoleFiltered(ctx, cnf, conn, db, o.readDBRole.Name, cnf.DatabaseRolePrivileges[ModeRead], nil) {
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
	// Called from within Grupin.setDBRoleGrants
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
				Database:          o.readDBRole.Database,
				GrantedTo:         ObjTpDatabaseRole,
				GrantedToDatabase: o.readDBRole.Database,
				GrantedToName:     o.readDBRole.Name,
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
					Database:          o.readDBRole.Database,
					GrantedTo:         ObjTpDatabaseRole,
					GrantedToDatabase: o.readDBRole.Database,
					GrantedToName:     o.readDBRole.Name,
				}) {
					return false
				}
			}
		}
	}
	for schema, schemaObjs := range o.Schemas {
		if !schemaObjs.pushToDoFutureGrants(yield, o.readDBRole, schema) {
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
			Database:          o.readDBRole.Database,
			GrantedTo:         ObjTpDatabaseRole,
			GrantedToDatabase: o.readDBRole.Database,
			GrantedToName:     o.readDBRole.Name,
		}) {
			return false
		}
	}
	for schema, schemaObjs := range o.Schemas {
		if !schemaObjs.pushToDoGrants(yield, o.readDBRole, schema) {
			return false
		}
	}
	return true
}

func (o AggDBObjs) pushToDoFutureRevokes(yield func(FutureGrant) bool) bool {
	for _, g := range o.revokeFutureGrantsToReadDBRole {
		if !yield(g) {
			return false
		}
	}
	return true
}

func (o AggDBObjs) pushToDoRevokes(yield func(Grant) bool) bool {
	for _, g := range o.revokeGrantsToReadDBRole {
		if !yield(g) {
			return false
		}
	}
	return true
}
