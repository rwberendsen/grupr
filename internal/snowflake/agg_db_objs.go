package snowflake

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/rwberendsen/grupr/internal/semantics"
)

type AggDBObjs struct {
	Schemas         map[semantics.Ident]AggSchemaObjs
	MatchAllSchemas bool
	MatchAllObjects bool

	// Set when (future) grants are set
	readDBRole      DatabaseRole
	isReadDBRoleNew bool // if true, then no need to query grants

	isMonitorGranted bool

	// Does the DB role have unmanaged privileges?
	hasUnmanagedGrantsReadDBRole bool

	// Grants to the readDBRole
	isPrivilegeGrantedOnFutureSchemasToReadDBRole [2]bool
	// Small lookup table, first index rows, second index columns
	//   		0: PrvSelect	1: PrvSelectErrorTable	2: PrvRefernces
	// 0: ObjTpTable
	// 1: ObjTpView
	// 2: ObjTpMaterializedView
	//
	// Note: PrvSelectErrorTable is only applicable to ObjTpTable,
	// so this representation does waste a little space.
	isPrivilegeOnFutureObjectGrantedToReadDBRole [7]bool
	revokeGrantsToReadDBRole                     []Grant
	revokeFutureGrantsToReadDBRole               []FutureGrant

	// Has the readDBRole been granted to the consuming ProductDTAPs already?
	// TODO: can this be a struct{} value type?
	consumedByGranted map[semantics.ProductDTAPID][2]bool

	// Grants to the product read and write roles; only used if this AggDBObjs is part of a product level interface
	isReadDBRoleGrantedToProductRole [2]bool // directly set from within Grupin.setDBRoleGrants

	// Grants to the product write role; only used if this AggDBObjs is part of a product level interface
	isCreateObjectOnFutureSchemasGrantedToProductWriteRole [3]bool // 0: ObjTpTable, 1: ObjTpView, 2: ObjTpMaterializedView

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
				o.isPrivilegeGrantedOnFutureSchemasToReadDBRole[0] = true
			case PrvMonitor:
				o.isPrivilegeGrantedOnFutureSchemasToReadDBRole[1] = true
			}
		case ObjTpTable:
			switch g.Privileges[0].Privilege {
			case PrvSelect:
				o.isPrivilegeOnFutureObjectGrantedToReadDBRole[0] = true
			case PrvSelectErrorTable:
				o.isPrivilegeOnFutureObjectGrantedToReadDBRole[1] = true
			case PrvReferences:
				o.isPrivilegeOnFutureObjectGrantedToReadDBRole[2] = true
			}
		case ObjTpView:
			switch g.Privileges[0].Privilege {
			case PrvSelect:
				o.isPrivilegeOnFutureObjectGrantedToReadDBRole[3] = true
			case PrvReferences:
				o.isPrivilegeOnFutureObjectGrantedToReadDBRole[4] = true
			}
		case ObjTpMaterializedView:
			switch g.Privileges[0].Privilege {
			case PrvSelect:
				o.isPrivilegeOnFutureObjectGrantedToReadDBRole[5] = true
			case PrvReferences:
				o.isPrivilegeOnFutureObjectGrantedToReadDBRole[6] = true
			}
		}
	case ModeWrite:
		switch g.GrantedOn {
		case ObjTpSchema:
			switch pc := g.Privileges[0]; pc.Privilege {
			case PrvCreate:
				switch pc.CreateObjectType {
				case ObjTpTable:
					o.isCreateObjectOnFutureSchemasGrantedToProductWriteRole[0] = true
				case ObjTpView:
					o.isCreateObjectOnFutureSchemasGrantedToProductWriteRole[1] = true
				case ObjTpMaterializedView:
					o.isCreateObjectOnFutureSchemasGrantedToProductWriteRole[2] = true
				}
			}
		}
	}
	return o
}

func (o AggDBObjs) hasFutureGrantTo(m Mode, grantedOn ObjType, pc PrivilegeComplete) bool {
	// Used for setting if grants on future objects in AggDBObjs have been
	// granted to either the readDBRole (ModeRead) or the ProductWriteRole
	// (ModeWrite)
	switch m {
	case ModeRead:
		switch grantedOn {
		case ObjTpSchema:
			switch pc.Privilege {
			case PrvUsage:
				return o.isPrivilegeGrantedOnFutureSchemasToReadDBRole[0]
			case PrvMonitor:
				return o.isPrivilegeGrantedOnFutureSchemasToReadDBRole[1]
			}
		case ObjTpTable:
			switch pc.Privilege {
			case PrvSelect:
				return o.isPrivilegeOnFutureObjectGrantedToReadDBRole[0]
			case PrvSelectErrorTable:
				return o.isPrivilegeOnFutureObjectGrantedToReadDBRole[1]
			case PrvReferences:
				return o.isPrivilegeOnFutureObjectGrantedToReadDBRole[2]
			}
		case ObjTpView:
			switch pc.Privilege {
			case PrvSelect:
				return o.isPrivilegeOnFutureObjectGrantedToReadDBRole[3]
			case PrvReferences:
				return o.isPrivilegeOnFutureObjectGrantedToReadDBRole[4]
			}
		case ObjTpMaterializedView:
			switch pc.Privilege {
			case PrvSelect:
				return o.isPrivilegeOnFutureObjectGrantedToReadDBRole[5]
			case PrvReferences:
				return o.isPrivilegeOnFutureObjectGrantedToReadDBRole[6]
			}
		}
	case ModeWrite:
		switch grantedOn {
		case ObjTpSchema:
			switch pc.Privilege {
			case PrvCreate:
				switch pc.CreateObjectType {
				case ObjTpTable:
					return o.isCreateObjectOnFutureSchemasGrantedToProductWriteRole[0]
				case ObjTpView:
					return o.isCreateObjectOnFutureSchemasGrantedToProductWriteRole[1]
				case ObjTpMaterializedView:
					return o.isCreateObjectOnFutureSchemasGrantedToProductWriteRole[2]
				}
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

func (o AggDBObjs) setRevokeGrantTo(m Mode, g Grant) AggDBObjs {
	// Used only for ModeRead at the moment, but, in the future we will have a ModeOps, most likely
	// Note that ModeWrite privileges to be revoked are stored in ProductDTAP
	if m != ModeRead {
		panic("not implemented")
	}
	o.revokeGrantsToReadDBRole = append(o.revokeGrantsToReadDBRole, g)
	return o
}

func (o AggDBObjs) setDatabaseRole(ctx context.Context, semCnf *semantics.Config, cnf *Config, conn *sql.DB, pID string, dtap string, iID string,
	db semantics.Ident, databaseRoles map[DatabaseRole]struct{}) (AggDBObjs, error) {
	o.readDBRole = NewDatabaseRole(semCnf, pID, dtap, iID, ModeRead, db)
	if _, ok := databaseRoles[o.readDBRole]; !ok {
		if err := o.readDBRole.Create(ctx, cnf, conn); err != nil {
			return o, err
		}
		o.isReadDBRoleNew = true
	}
	return o, nil
}

func (o AggDBObjs) setFutureGrants(ctx context.Context, semCnf *semantics.Config, cnf *Config, conn *sql.DB, pID string, dtap string, iID string,
	db semantics.Ident, oms semantics.ObjMatchers, databaseRoles map[DatabaseRole]struct{}) (AggDBObjs, error) {
	o, err := o.setDatabaseRole(ctx, semCnf, cnf, conn, pID, dtap, iID, db, databaseRoles)
	if err != nil {
		return o, err
	}
	if !o.isReadDBRoleNew {
		for g, err := range QueryFutureGrantsToDBRoleFiltered(ctx, conn, db, o.readDBRole.Name, cnf.ObjectPrivilegesRead, nil) {
			if err != nil {
				return o, err
			}

			if g.Database != db {
				// This grant should not be granted to this particular database role, this should not be possible in Snowflake atm (May 2026)
				return o, fmt.Errorf("privilege granted to database role on future objects from different database")
			}

			switch g.GrantedIn {
			case ObjTpDatabase:
				switch g.GrantedOn {
				case ObjTpSchema:
					if o.MatchAllSchemas {
						o = o.setFutureGrantTo(ModeRead, g)
						continue
					}
					// We need to revoke
				case ObjTpTable, ObjTpView, ObjTpMaterializedView:
					if o.MatchAllObjects {
						o = o.setFutureGrantTo(ModeRead, g)
						continue
					}
					// We need to revoke
				}
			case ObjTpSchema:
				switch g.GrantedOn {
				case ObjTpTable, ObjTpView, ObjTpMaterializedView:
					if o.hasSchema(g.Schema) {
						if o.Schemas[g.Schema].MatchAllObjects {
							o.Schemas[g.Schema] = o.Schemas[g.Schema].setFutureGrantTo(ModeRead, g)
							continue
						}
						// We need to revoke
					} else {
						// A rare oddity. A schema was added after we loaded account objects,
						// and future grants were granted in it to our database role, no less.
						// But, if the YAML indicates this is correct, we will leave the grant intact
						if oms.MatchAllObjectsInSchema(db, g.Schema) {
							continue
						}
						// We need to revoke
					}
				}
			}
			// If we are still here, we need to revoke
			o = o.setRevokeFutureGrantTo(ModeRead, g)
		}
	}
	return o, nil
}

func (o AggDBObjs) setGrants(ctx context.Context, semCnf *semantics.Config, cnf *Config, conn *sql.DB, db semantics.Ident, oms semantics.ObjMatchers) (AggDBObjs, error) {
	if !o.isReadDBRoleNew {
		// First, check for unmanaged grants, and keep track of in which schemas the database role holds unmanaged grants;
		// Since we don't know how to parse grants that grupr does not manage, if there is even a single unmanaged
		// grant, no matter what it is, we are not going to revoke usage or monitor from any databases or schemas; even
		// if those databases and schemas do not match any object expression from the YAML; even if the objects are
		// matched by a different product its YAML. After
		// all, it could be that this would render ineffective an unmanaged privilege on an object in such a schema,
		// and grupr would break some access that was legitimately put in place by sysadmins.
		// We should not revoke USAGE on these schemas from the database role, not even if the schema is disjoint from the YAML.
		//
		// Note that write privileges on objects are unmanaged from the perspective of a database role. If sysadmins
		// would grant them to grupr managed database roles, despite best practices, they will have to revoke them also
		// in case they want grupr to drop the role when it is no longer needed.
		for _, err := range QueryGrantsToDBRoleFilteredLimit(ctx, cnf, conn, db, o.readDBRole.Name, nil, cnf.ObjectPrivilegesRead, 1) {
			if err != nil {
				return o, err
			}
			o.hasUnmanagedGrantsReadDBRole = true
		}

		// Second, check for managed grants
		for g, err := range QueryGrantsToDBRoleFiltered(ctx, cnf, conn, db, o.readDBRole.Name, cnf.ObjectPrivilegesRead, nil) {
			if err != nil {
				return o, err
			}

			if g.Database != db {
				// This grant should not be granted to this particular database role, that should not be
				// possible in Snowflake, at the moment
				return o, fmt.Errorf("privilege granted to database role on object from different database")
			}

			switch ot := g.GrantedOn; ot {
			case ObjTpDatabase:
				switch g.Privileges[0].Privilege {
				case PrvMonitor:
					o.isMonitorGranted = true
					continue
				case PrvUsage:
					// PrvUsage comes out of the box, we do not grant it, so no need to mark it as present
					continue
				}
			case ObjTpSchema:
				switch g.Privileges[0].Privilege {
				case PrvUsage, PrvMonitor:
					if oms.DisjointFromSchema(g.Database, g.Schema) {
						if o.hasUnmanagedGrantsReadDBRole {
							// We need to keep this grant
							continue
						}
						// We need to revoke
					} else {
						if o.hasSchema(g.Schema) {
							o.Schemas[g.Schema] = o.Schemas[g.Schema].setGrantTo(ModeRead, g)
						}
						// Either way, if the schema is matched in the YAML, even if it was not there
						// when we refreshed objects, the grant is fine, we keep it.
						continue
					}
				}
			case ObjTpTable, ObjTpView, ObjTpMaterializedView:
				switch g.Privileges[0].Privilege {
				case PrvSelect, PrvReferences, PrvSelectErrorTable:
					if !oms.DisjointFromObject(g.Database, g.Schema, g.Object) {
						if o.hasObject(g.Schema, g.Object) {
							// We call the String method on the object type to normalize AggObjAttr.ObjectType from
							// ObjTpHybridTable to ObjTpTable
							if o.Schemas[g.Schema].Objects[g.Object].ObjectType.String() != ot.String() {
								// A (hybrid) table may have been dropped and a (materialized) view with the same name
								// created or vice versa A good reason to refresh the product
								return o, ErrObjectNotExistOrAuthorized
							}
							o.Schemas[g.Schema].Objects[g.Object] = o.Schemas[g.Schema].Objects[g.Object].setGrantTo(ModeRead, g)
						}
						// And we need to keep this grant
						continue
					}
					// We need to revoke
				}
			}
			// If we are still here, we need to revoke this grant
			o = o.setRevokeGrantTo(ModeRead, g)
		}
	}
	return o, nil
}

func (o AggDBObjs) setConsumedByGranted(m Mode, pdID semantics.ProductDTAPID) AggDBObjs {
	// Called from within Grupin.setDBRoleGrants
	if o.consumedByGranted == nil {
		o.consumedByGranted = map[semantics.ProductDTAPID][2]bool{}
	}
	o.consumedByGranted[pdID] = setFlagMode(o.consumedByGranted[pdID], m)
	return o
}

func (o AggDBObjs) pushToDoFutureGrants(yield func(FutureGrant) bool, mots map[ObjType]bool) bool {
	// All future read privileges; write privileges are collected from a ProductDTAP method directly
	if o.MatchAllSchemas {
		prvs := []PrivilegeComplete{}
		for _, pc := range [2]PrivilegeComplete{
			PrivilegeComplete{Privilege: PrvUsage},
			PrivilegeComplete{Privilege: PrvMonitor},
		} {
			if !o.hasFutureGrantTo(ModeRead, ObjTpSchema, pc) {
				prvs = append(prvs, pc)
			}
		}
		if len(prvs) > 0 {
			if !yield(FutureGrant{
				Privileges:        prvs,
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
		for ot := range mots {
			prvs := []PrivilegeComplete{}
			candidatePrvs := []PrivilegeComplete{
				PrivilegeComplete{Privilege: PrvSelect},
				PrivilegeComplete{Privilege: PrvReferences},
			}
			if ot == ObjTpTable {
				// Note that this privilege does not apply to hybrid tables, but we count on
				// Snowflake not giving problems when folks create a hybrid table
				candidatePrvs = append(candidatePrvs, PrivilegeComplete{Privilege: PrvSelectErrorTable})
			}
			for _, pc := range candidatePrvs {
				if !o.hasFutureGrantTo(ModeRead, ot, pc) {
					prvs = append(prvs, pc)
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
		if !schemaObjs.pushToDoFutureGrants(yield, o.readDBRole, schema, mots) {
			return false
		}
	}
	return true
}

func (o AggDBObjs) pushToDoGrants(yield func(Grant) bool) bool {
	if !o.isMonitorGranted {
		if !yield(Grant{
			Privileges:        []PrivilegeComplete{PrivilegeComplete{Privilege: PrvMonitor}},
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

func (o AggDBObjs) pushExternalGrants(ctx context.Context, semCnf *semantics.Config, conn *sql.DB, db semantics.Ident, yield func(Grant, error) bool) bool {
	if o.MatchAllObjects {
		for g, err := range QueryExternalGrantsOnDB(ctx, semCnf, conn, db) {
			if !yield(g, err) {
				return false
			}
		}
	}
	for schema, schemaObjs := range o.Schemas {
		if !schemaObjs.pushExternalGrants(ctx, semCnf, conn, db, schema, yield) {
			return false
		}
	}
	return true
}
