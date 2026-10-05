package snowflake

import (
	"context"
	"database/sql"
	"iter"
	"maps"
	"slices"
	"strings"

	"github.com/rwberendsen/grupr/internal/semantics"
)

type Interface struct {
	ObjectMatchers   semantics.ObjMatchers
	GlobalUserGroups map[string]struct{}
	UserGroupMapping semantics.UserGroupMapping
	ConsumedBy       map[semantics.ProductDTAPID]struct{}

	// Granular accountObjects by ObjExpr; will be discarded after aggregate() is called
	accountObjects map[semantics.ObjExpr]AccountObjs

	// Computed by aggregate()
	objectCountsByUserGroup map[string]map[ObjType]int // "" means shared by usergroups of interface, if any

	// Computed by aggregate()
	aggAccountObjects AggAccountObjs

	// For use in pushObjectCounts
	globalUserGroupsStr string
}

func NewInterface(dtap string, iSem semantics.InterfaceMetadata, um semantics.UserGroupMapping) *Interface {
	i := &Interface{
		ObjectMatchers:   semantics.ObjMatchers{},
		UserGroupMapping: um,
	}
	// Just take what you need from own DTAP
	for e, om := range iSem.ObjectMatchers {
		if om.DTAP == dtap {
			i.ObjectMatchers[e] = om
		}
	}
	// Set Global user groups and userGroupStr
	if iSem.UserGroups != nil {
		i.GlobalUserGroups = map[string]struct{}{}
		for u := range iSem.UserGroups {
			i.GlobalUserGroups[i.UserGroupMapping[u]] = struct{}{}
		}
		i.globalUserGroupsStr = strings.Join(slices.Sorted(maps.Keys(i.GlobalUserGroups)), ",")
	}
	if iSem.ConsumedBy != nil {
		// this is an interface (not a product-level one)
		i.ConsumedBy = map[semantics.ProductDTAPID]struct{}{}
		for dtapSem, pdIDs := range iSem.ConsumedBy {
			if dtapSem == dtap {
				for pdID := range pdIDs {
					i.ConsumedBy[pdID] = struct{}{}
				}
			}
		}
	}
	return i
}

func (i *Interface) recalcObjectsFromMatched(m map[semantics.ObjExpr]*matchedAccountObjs) {
	// Called from the product level only
	i.accountObjects = map[semantics.ObjExpr]AccountObjs{} // (re)set
	for e, om := range i.ObjectMatchers {
		tmpAccountObjs := newAccountObjsFromMatched(m[e])
		i.accountObjects[e] = newAccountObjs(tmpAccountObjs, om)
	}
}

func (i *Interface) recalcObjects(m map[semantics.ObjExpr]AccountObjs) {
	// Called from the interface level, work with SubsetOf here
	i.accountObjects = map[semantics.ObjExpr]AccountObjs{} // (re)set
	for e, om := range i.ObjectMatchers {
		i.accountObjects[e] = newAccountObjs(m[om.SubsetOf], om)
	}
}

func (i *Interface) aggregate() {
	i.setCountsByUserGroup()
	i.setAggAccountObjects()
}

func (i *Interface) setCountsByUserGroup() {
	i.objectCountsByUserGroup = map[string]map[ObjType]int{}
	for e, om := range i.ObjectMatchers {
		var globalUserGroup string
		if om.UserGroup != "" {
			globalUserGroup = i.UserGroupMapping[om.UserGroup]
		}
		if i.objectCountsByUserGroup[globalUserGroup] == nil {
			i.objectCountsByUserGroup[globalUserGroup] = map[ObjType]int{}
		}
		i.objectCountsByUserGroup[globalUserGroup][ObjTpTable] += i.accountObjects[e].countByObjType(ObjTpTable)
		i.objectCountsByUserGroup[globalUserGroup][ObjTpView] += i.accountObjects[e].countByObjType(ObjTpView)
	}
}

func (i *Interface) setAggAccountObjects() {
	sum := AccountObjs{}
	for _, o := range i.accountObjects {
		sum = sum.add(o)
	}
	i.aggAccountObjects = newAggAccountObjs(sum)
	i.accountObjects = nil // reset, we do not need it anymore, and maps referenced inside this data structure may have been altered while summing
}

func (i *Interface) setFutureGrants(ctx context.Context, semCnf *semantics.Config, cnf *Config, conn *sql.DB, pID string, dtap string, iID string, c *accountCache) error {
	for db, dbObjs := range i.aggAccountObjects.DBs {
		if !c.hasDB(db) {
			return ErrObjectNotExistOrAuthorized // db may have been dropped concurrently
		}
		dbObjs, err := dbObjs.setFutureGrants(ctx, semCnf, cnf, conn, pID, dtap, iID, db, i.ObjectMatchers, c.dbs[db].dbRoles)
		if err != nil {
			return err
		}
		i.aggAccountObjects.DBs[db] = dbObjs
	}
	return nil
}

func (i *Interface) setGrants(ctx context.Context, semCnf *semantics.Config, cnf *Config, conn *sql.DB, c *accountCache) error {
	for db, dbObjs := range i.aggAccountObjects.DBs {
		if !c.hasDB(db) {
			return ErrObjectNotExistOrAuthorized // db may have been dropped concurrently
		}
		dbObjs, err := dbObjs.setGrants(ctx, semCnf, cnf, conn, db, i.ObjectMatchers)
		if err != nil {
			return err
		}
		i.aggAccountObjects.DBs[db] = dbObjs
	}
	return nil
}

func (i *Interface) pushToDoFutureGrants(yield func(FutureGrant) bool, mots map[ObjType]bool) bool {
	for _, dbObjs := range i.aggAccountObjects.DBs {
		if !dbObjs.pushToDoFutureGrants(yield, mots) {
			return false
		}
	}
	return true
}

func (i *Interface) pushToDoGrants(yield func(Grant) bool) bool {
	for _, dbObjs := range i.aggAccountObjects.DBs {
		if !dbObjs.pushToDoGrants(yield) {
			return false
		}
	}
	return true
}

func (i *Interface) pushToDoDBRoleGrants(yield func(Grant) bool, doProd bool, m map[semantics.ProductDTAPID]*ProductDTAP) bool {
	for db, dbObjs := range i.aggAccountObjects.DBs {
		for pdID := range i.ConsumedBy {
			if doProd == m[pdID].IsProd {
				for _, pr := range [2]ProductRole{m[pdID].ReadRole, m[pdID].WriteRole} {
					if !dbObjs.consumedByGranted[pdID][pr.Mode.getIdx()] {
						if !yield(Grant{
							Privileges:    []PrivilegeComplete{PrivilegeComplete{Privilege: PrvUsage}},
							GrantedOn:     ObjTpDatabaseRole,
							Database:      db,
							GrantedRole:   dbObjs.readDBRole.Name,
							GrantedTo:     ObjTpRole,
							GrantedToName: pr.ID,
						}) {
							return false
						}
					}
				}
			}
		}
	}
	return true
}

func (i *Interface) pushToDoFutureRevokes(yield func(FutureGrant) bool) bool {
	for _, dbObjs := range i.aggAccountObjects.DBs {
		if !dbObjs.pushToDoFutureRevokes(yield) {
			return false
		}
	}
	return true
}

func (i *Interface) pushToDoRevokes(yield func(Grant) bool) bool {
	for _, dbObjs := range i.aggAccountObjects.DBs {
		if !dbObjs.pushToDoRevokes(yield) {
			return false
		}
	}
	return true
}

func (i *Interface) pushObjectCounts(yield func(ObjCountsRow) bool, pdID semantics.ProductDTAPID, iid string) bool {
	for ug, countsByObjType := range i.objectCountsByUserGroup {
		r := ObjCountsRow{
			ProductID:   pdID.ProductID,
			DTAP:        pdID.DTAP,
			InterfaceID: iid,
			UserGroups:  ug,
			TableCount:  countsByObjType[ObjTpTable],
			ViewCount:   countsByObjType[ObjTpView],
		}
		if ug == "" {
			r.UserGroups = i.globalUserGroupsStr
		}
		if !yield(r) {
			return false
		}
	}
	return true
}

func (i *Interface) getCurrentOwners(productID string, dtap string, semCnf *semantics.Config, cnf *Config,
	userManagedOwners func(semantics.ProductDTAPID) map[semantics.Ident]struct{}) (map[semantics.Ident]struct{}, error) {
	userManagedOwnersOfObjects := map[semantics.Ident]struct{}{}
	for db, dbObjs := range i.aggAccountObjects.DBs {
		for _, schemaObjs := range dbObjs.Schemas {
			for _, aggObjAttr := range schemaObjs.Objects {
				// Ignore system defined roles
				if slices.Contains(cnf.SystemDefinedRoles, aggObjAttr.Owner) {
					continue
				}
				// Deal with grupr managed roles that are the current owner
				if strings.HasPrefix(string(aggObjAttr.Owner), string(semCnf.Prefix)) {
					r, err := newProductRoleFromIdent(semCnf, aggObjAttr.Owner)
					if err != nil {
						// In this case, it would have to be a database role, or else other roles
						// exist sharing the grupr prefix, which would be a good reason to crash
						if _, err = newDatabaseRoleFromIdent(semCnf, db, aggObjAttr.Owner); err != nil {
							return userManagedOwnersOfObjects, err
						}
						// Okay, so it was a database role that owned the object. Not something sysadmins
						// should have done. Not something grupr would do. But we'll just not add any
						// previous user managed owning roles. Ownership of the object will be sorted out
						// for this object cause it was matched by this product: the write role will
						// claim ownership of it.
					}

					// Now, we deal with a special case
					if r.Mode == ModeWrite && (r.ProductID != productID || r.DTAP != dtap) {
						// So, another write role owned this object before, we need to check what user managed roles
						// have been granted this other write role; they would lose ownership of the object if we
						// would claim it; so we need to grant our write role to those user managed roles, if any
						//
						// We do this as a service. It is a normal thing that can happen when people rename a product
						// in the YAML, i.e., change it's product id. Or when an object matching expression moves
						// from one product to another.
						for curOwner := range userManagedOwners(semantics.ProductDTAPID{ProductID: r.ProductID, DTAP: r.DTAP}) {
							userManagedOwnersOfObjects[curOwner] = struct{}{}
						}
					}

					// Else, this really is not a role we'd want to grant our product write role to.
					continue
				}

				// It's a user managed role, we add it
				userManagedOwnersOfObjects[aggObjAttr.Owner] = struct{}{}
			}
		}
	}
	return userManagedOwnersOfObjects, nil
}

func (i *Interface) getToDoOwnershipGrants(role semantics.Ident) iter.Seq[Grant] {
	return func(yield func(Grant) bool) {
		for db, dbObjs := range i.aggAccountObjects.DBs {
			for schema, schemaObjs := range dbObjs.Schemas {
				for obj, objAttr := range schemaObjs.Objects {
					if role != objAttr.Owner { // if you are the owner already, no need to grant again
						ot := objAttr.ObjectType
						if ot == ObjTpHybridTable {
							// In snowflake GRANT <privileges> ..., HYBRID TABLE is not a recongnized object type
							ot = ObjTpTable
						}
						if !yield(Grant{
							Privileges:    []PrivilegeComplete{PrivilegeComplete{Privilege: PrvOwnership}},
							GrantedOn:     objAttr.ObjectType,
							Database:      db,
							Schema:        schema,
							Object:        obj,
							GrantedTo:     ObjTpRole,
							GrantedToName: role,
						}) {
							return
						}
					}
				}
			}
		}
	}
}

func (i *Interface) grantOwnershipTo(ctx context.Context, cnf *Config, conn *sql.DB, role semantics.Ident) error {
	// Grant ownership of objects to role (copy current grants, to cause minimal disturbance)
	// We don't do ownership grants in batches, cause they can take longer due to copying outbound grants;
	// they can even time-out for that reason, as mentioned in a 2025 version of Snowflake its documentation.
	// We do them one by one.
	return DoGrantsIndividually(ctx, cnf, conn, i.getToDoOwnershipGrants(role))
}

func (i *Interface) manageAccessExclusively(ctx context.Context, semCnf *semantics.Config, cnf *Config, conn *sql.DB) error {
	// So here we just get all the 'ALL (PRIVILEGES) grants that we need to revoke from an iterator defined on
	// aggAccountObjects, and we throw it at a DoRevokes function.
	return DoRevokesExitOnInputErrors(ctx, cnf, conn, i.aggAccountObjects.getExternalGrants(ctx, semCnf, conn))
}
