package snowflake

import (
	"context"
	"database/sql"
	"fmt"
	"iter"
	"log"
	"slices"
	"strings"

	"github.com/rwberendsen/grupr/internal/semantics"
	"github.com/rwberendsen/grupr/internal/util"
)

/*
In product_dtap__objects.go, we have ProductDTAP methods that deal mostly with (privileges on) objects
*/

func (pd *ProductDTAP) refresh(ctx context.Context, semCnf *semantics.Config, cnf *Config, conn *sql.DB, c *accountCache) error {
	if err := pd.refresh_(ctx, semCnf, cnf, conn, c); err != nil {
		return err
	}
	pd.recalcObjects() // will reset all accountObjs
	// Reset other properties of pd that depend on which objects where matched
	pd.toRevokeFutureObjects = []FutureGrant{}
	pd.toRevokeObjects = []Grant{}
	pd.toTransferOwnership = []Grant{}
	return nil
}

func (pd *ProductDTAP) refresh_(ctx context.Context, semCnf *semantics.Config, cnf *Config, conn *sql.DB, c *accountCache) error {
	for {
		pd.refreshCount += 1
		if pd.refreshCount > cnf.MaxProductDTAPRefreshes {
			return fmt.Errorf("Max product refresh count reached")
		}
		util.SleepContext(ctx, 1<<pd.refreshCount-1) // exponential backoff
		if err := pd.refreshObjExprs(ctx, semCnf, cnf, conn, c); err != ErrObjectNotExistOrAuthorized {
			return err
		}
	}
}

func (pd *ProductDTAP) refreshObjExprs(ctx context.Context, semCnf *semantics.Config, cnf *Config, conn *sql.DB, c *accountCache) error {
	for e, om := range pd.ObjectMatchers {
		if err := c.match(ctx, semCnf, cnf, conn, om, pd.matchedAccountObjects[e]); err != nil {
			return err
		}
	}
	return nil
}

func (pd *ProductDTAP) recalcObjects() {
	pd.Interface.recalcObjectsFromMatched(pd.matchedAccountObjects)
	for _, v := range pd.Interfaces {
		v.recalcObjects(pd.accountObjects)
		v.aggregate() // this will free memory held by AccountObjs by ObjExpr
	}
	pd.Interface.aggregate() // we needed to hold on to AccountObjs by ObjExpr until we derived all interface objects
}

func (pd *ProductDTAP) setGrantActionsFutureObjectsWriteRole(ctx context.Context, cnf *Config, conn *sql.DB, productRoles map[ProductRole]struct{}) error {
	// This function covers setting the todo and already done actions regarding privileges on future objects for the product write role
	// It does not cover compute privileges, and it does not cover usage of database roles.
	if _, ok := productRoles[pd.WriteRole]; !ok && cnf.DryRun {
		return nil
	}
	for g, err := range QueryFutureGrantsToRoleFiltered(ctx, conn, pd.WriteRole.ID,	cnf.ObjectPrivileges, nil) {
		if err != nil {
			return err
		}
		switch g.GrantedIn {
		case ObjTpDatabase:
			switch g.GrantedOn {
			case ObjTpSchema:
				switch g.Privileges[0].Privilege {
				case PrvCreate:
					// Should we have this grant?
					if pd.Interface.ObjectMatchers.MatchAllSchemasInDB(g.Database) {
						// If yes, then, if we also have matched the object, mark on it that privilege on future objects was already granted
						if dbObjs, ok := pd.Interface.aggAccountObjects.DBs[g.Database]; ok {
							dbObjs.setFutureGrantTo(ModeWrite, g)
						}
						// If we did not match the object, there are two possibilities:
						// 1. The schema was created after we matched objects, but before we queried future grants on
						// the write role.
						// 2. Despite the fact that the grant_on column matches our grant template, the object is not of
						// a kind that grupr is managing after all. At the moment, that should not occur, but it may
						// occur in the future as Snowflake is changing continuously. 
						// In both cases, it would be okay to leave the grant intact.
						continue
					}
				case PrvUsage, PrvMonitor:
					// Grupr does not normally assign these privileges, but sysadmins are encouraged to do so in case
					// they want to grant privileges on objects in schemas to the write role that grupr does not (yet)
					// manage. For that reason, we will leave these grants intact.
					continue
				}
			}
		}
		// If we are still here, this grant will be a revoke candidate.
		pd.toRevokeFutureObjects = append(pd.toRevokeFutureObjects, g)
	}
	return nil
}

func (pd *ProductDTAP) setGrantActionsObjectsWriteRole(ctx context.Context, cnf *Config, conn *sql.DB,
	grupinDisjointFromObject func(semantics.Ident, semantics.Ident, semantics.Ident) bool, productRoles map[ProductRole]struct{}) error {
	// This function covers setting the todo and already done actions regarding privileges on objects for the product write role
	// It does not cover compute privileges, and it does not cover usage of database roles.
	if _, ok := productRoles[pd.WriteRole]; !ok && cnf.DryRun {
		return nil
	}
	for g, err := range QueryGrantsToRoleFiltered(ctx, cnf, conn, pd.WriteRole.ID, cnf.ObjectPrivileges, nil) {
		if err != nil {
			return err
		}
		switch pc := g.Privileges[0]; pc.Privilege {
		case PrvCreate:
			if !pd.Interface.ObjectMatchers.DisjointFromSchema(g.Database, g.Schema) {
				if dbObjs, ok := pd.Interface.aggAccountObjects.DBs[g.Database]; ok {
					if schemaObjs, ok := dbObjs.Schemas[g.Schema]; ok {
						dbObjs.Schemas[g.Schema] = schemaObjs.setGrantTo(ModeWrite, g)
					}
				}
				// Whether or not we actually found the schema in the database, the grant is fine
				continue
			}
		case PrvOwnership:
			if !pd.Interface.ObjectMatchers.DisjointFromObject(g.Database, g.Schema, g.Object) {
				if schemaObjs, ok := pd.Interface.aggAccountObjects.GetSchema(g.Database, g.Schema); ok {
					if aggObjAttr, ok := schemaObjs.Objects[g.Object]; ok {
						schemaObjs.Objects[g.Object] = aggObjAttr.setGrantTo(ModeWrite, g)
					}
				}
			} else if grupinDisjointFromObject(g.Database, g.Schema, g.Object) {
				// There will be no other product claiming ownership of this object, we need to
				// transfer its ownership to a role that is not managed by grupr.
				// Note that when we refreshed, toTransferOwnership was reset to an empty slice
				pd.toTransferOwnership = append(pd.toTransferOwnership, g)
			}
			continue // There is no revoking ownersip, so, continue either way
		case PrvUsage, PrvMonitor:
			// Grupr does not normally assign these privileges, but sysadmins are encouraged to do so in case
			// they want to grant privileges on objects in schemas to the write role that grupr does not (yet)
			// manage. For that reason, we will leave these grants intact.
			//
			// Note that it does not matter if usage / monitor was granted on a database or a schema.
			continue
		}
		// If we are still here, this grant is a candidate for being revoked
		pd.toRevokeObjects = append(pd.toRevokeObjects, g)
	}
	return nil
}

func (pd *ProductDTAP) setUserManagedOwnersOfObjects(semCnf *semantics.Config, cnf *Config,
	userManagedOwners func(semantics.ProductDTAPID) map[semantics.Ident]struct{}) error {
	pd.userManagedOwnersOfObjects = map[semantics.Ident]struct{}{}
	for db, dbObjs := range pd.Interface.aggAccountObjects.DBs {
		for _, schemaObjs := range dbObjs.Schemas {
			for _, aggObjAttr := range schemaObjs.Objects {
				if slices.Contains(cnf.SystemDefinedRoles, aggObjAttr.Owner) {
					continue
				}
				if strings.HasPrefix(string(aggObjAttr.Owner), string(semCnf.Prefix)) {
					r, err := newProductRoleFromIdent(semCnf, aggObjAttr.Owner); {
					if err != nil {
						// In this case, it would have to be a database role, or else other roles
						// exist sharing the grupr prefix, this would be a good reason to crash
						if _, err = newDatabaseRoleFromString(semCnf, db, aggObjAttr.Owner); err != nil {
							return err
						}
						// Okay, so it was a database role that owned the object. Not something sysadmins
						// should have done. Not something grupr would do. But we'll just not add any
						// previous user managed owning roles. Ownership of the object will be sorted out
						// for this object cause it was matched by this product: the write role will 
						// claim ownership of it.
					}
					if r.Mode == ModeWrite && (r.ProductID != pd.ProductID || r.DTAP != pd.DTAP) {
						// So, another write role owned this object before, we need to check what user managed roles
						// have been granted this other write role; they would lose ownership of the object if we
						// would claim it; so we need to grant our write role to those user managed roles, if any
						//
						// We do this as a service. It is a normal thing that can happen when people rename a product
						// in the YAML, i.e., change it's product id. Or when an object matching expression moves
						// from one product to another.
						for curOwner := range userManagedOwners(semantics.ProductDTAPID{ProductID: r.ProductID, DTAP: r.DTAP}) {
							pd.userManagedOwnersOfObjects[curOwner] = struct{}{}
						}
					}
					continue
				}
				// It's a user managed role, we add it
				pd.userManagedOwnersOfObjects[aggObjAttr.Owner] = struct{}{}
			}
		}
	}
	return nil
}

func (pd *ProductDTAP) grant_(ctx context.Context, semCnf *semantics.Config, cnf *Config, conn *sql.DB, productRoles map[ProductRole]struct{},
	grupinDisjointFromObject func(semantics.Ident, semantics.Ident, semantics.Ident) bool,
	userManagedOwners func(semantics.ProductDTAPID) map[semantics.Ident]struct{}, c *accountCache) error {
	// First get the objects that are there in the account
	if err := pd.refresh(ctx, semCnf, cnf, conn, c); err != nil {
		return err
	}

	// Write grants go first, so that we do not have to copy all the read privileges we're about to set when granting ownership.
	// As with read grants, future grants go first
	if err := pd.setGrantActionsFutureObjectsWriteRole(ctx, cnf, conn, productRoles); err != nil {
		return err
	}
	if err := DoFutureGrants(ctx, cnf, conn, pd.getTodoGrantsFutureObjectsWriteRole()); err != nil {
		return err
	}

	// Now, regular grants to the write role
	if err := pd.setGrantActionsObjectsWriteRole(ctx, cnf, conn, grupinDisjointFromObject, productRoles); err != nil {
		return err
	}
	if err := DoGrants(ctx, cnf, conn, pd.getToDoGrantsObjectsWriteRole()); err != nil {
		return err
	}
	// We do ownership separately; we don't do them in batches, cause they can take longer due to copying outbound grants;
	// they can even time-out for that reason, as mentioned in a 2025 version of Snowflake its documentation. We do them
	// one by one.
	//
	// Note that we grant objects directly to the product role, not via intermediate database roles; this is because we do
	// not want database roles showing up as grantor, it's just confusing; objects should have a single owning actual role.
	//
	// Before we actually grant ownership to the write role, grant the write role itself to all the current owners of the objects
	// of interest. This way, they will not lose ownership, in fact, they will not lose any privilege, and running grupr will
	// not mess up any other processes that may be running.
	// We never revoke from any role; that is the job of sysadmins: when they are done with those roles, they can drop them,
	// or if the roles need to be retained for other purposes, they can revoke this product dtap role from that other role.
	if err := pd.setUserManagedOwnersOfObjects(semCnf, cnf, userManagedOwners); err != nil {
		return err
	}
	if err := DoGrants(ctx, cnf, conn, pd.getToDoGrantsOfWriteRoleToUserManagedRoles(semCnf, cnf)); err != nil {
		return err
	}
	// Then, make a second pass over the objects, and grant ownership to the write role.
	if err := DoGrantsIndividually(ctx, cnf, conn, pd.getToDoOwnershipGrants()); err != nil {
		return err
	}
	// At this point, granting of object privileges to the write role has been taken care of

	// Next, manage read privileges: we assign those to database roles.
	// Future grants first, so that as quickly as possible newly created objects will have correct privileges granted
	if err := pd.Interface.setFutureGrants(ctx, semCnf, cnf, conn, pd.ProductID, pd.DTAP, "", c); err != nil {
		return err
	}
	for iid, i := range pd.Interfaces {
		if err := i.setFutureGrants(ctx, semCnf, cnf, conn, pd.ProductID, pd.DTAP, iid, c); err != nil {
			return err
		}
	}
	if err := DoFutureGrants(ctx, cnf, conn, pd.getToDoFutureGrantsToDBRoles()); err != nil {
		return err
	}

	// Now, regular grants
	if err := pd.Interface.setGrants(ctx, semCnf, cnf, conn, c); err != nil {
		return err
	}
	for _, i := range pd.Interfaces {
		if err := i.setGrants(ctx, semCnf, cnf, conn, c); err != nil {
			return err
		}
	}
	if err := DoGrants(ctx, cnf, conn, pd.getToDoGrantsToDBRoles()); err != nil {
		return err
	}

	return nil
}

func (pd *ProductDTAP) revoke_(ctx context.Context, cnf *Config, conn *sql.DB) error {
	// We first revoke privileges from product roles, to stop the wrong roles from creating objects asap
	// As with read privileges, we start with future privileges
	if err := DoFutureRevokes(ctx, cnf, conn, slices.Values(pd.toRevokeFutureObjects)); err != nil {
		return err
	}
	if err := DoRevokes(ctx, cnf, conn, slices.Values(pd.toRevokeObjects)); err != nil {
		return err
	}
	// Now we transfer ownership of objects that should no longer be owned by Grupr-managed roles
	// First, we check if we can unambiguously do this. If not, we log a message and do not
	// transfer ownership; sysadmins need to make some changes in Snowflake first.
	var newOwner semantics.Ident
	var hasNewOwner bool
	if len(pd.writeRoleGrantedToUserManagedRoles) == 0 {
		// There were no user managed original owners before grupr ran, who
		// would lose privileges if we transfered ownership, and it should be safe
		// then to transfer ownership to SYSADMIN
		newOwner = semantics.Ident("SYSADMIN")
		hasNewOwner = true
	} else if len(pd.writeRoleGrantedToUserManagedRoles) == 1 {
		// Before grupr ran, this user managed role had OWNERSHIP indirectly over
		// all objects owned by the product write role. If we now transfer ownership
		// to SYSADMIN, this role would loose OWNERSHIP, potentially breaking a pipeline.
		// Instead, we "give back" ownership to this user managed role
		for k := range pd.writeRoleGrantedToUserManagedRoles {
			newOwner = k
		}
		hasNewOwner = true
	}
	if hasNewOwner {
		if err := DoGrantsIndividually(ctx, cnf, conn, pd.getTransferOwnershipGrants(newOwner)); err != nil {
			return err
		}
		pd.toTransferOwnership = []Grant{}
	}
	if !hasNewOwner && len(pd.toTransferOwnership) > 0 {
		log.Printf("WARN: multiple historic owners of objects that no longer should be owned by product '%s', dtap '%s', keeping ownership", pd.ProductID, pd.DTAP)
	}

	// Next, revoke read privileges from database roles
	// Future grants are revoked first, in case objects are being concurrently created, at least those
	// object will stop receiving incorrect grants first.
	if err := DoFutureRevokes(ctx, cnf, conn, pd.getToDoFutureRevokesFromDBRoles()); err != nil {
		return err
	}
	if err := DoRevokes(ctx, cnf, conn, pd.getToDoRevokesFromDBRoles()); err != nil {
		return err
	}
	return nil
}

func (pd *ProductDTAP) getTodoGrantsFutureObjectsWriteRole() iter.Seq[FutureGrant] {
	return func(yield func(FutureGrant) bool) {
		for db, dbObjs := range pd.Interface.aggAccountObjects.DBs {
			if dbObjs.MatchAllSchemas {
				prvs := []PrivilegeComplete{}
				for _, p := range [2]PrivilegeComplete{
					PrivilegeComplete{Privilege: PrvCreate, CreateObjectType: ObjTpTable},
					PrivilegeComplete{Privilege: PrvCreate, CreateObjectType: ObjTpView},
				} {
					if !dbObjs.hasFutureGrantTo(ModeWrite, ObjTpSchema, p) {
						prvs = append(prvs, p)
					}
				}
				if len(prvs) > 0 {
					if !yield(FutureGrant{
						Privileges:    prvs,
						GrantedOn:     ObjTpSchema,
						GrantedIn:     ObjTpDatabase,
						Database:      db,
						GrantedTo:     ObjTpRole,
						GrantedToName: pd.WriteRole.ID,
					}) {
						return
					}
				}
			}
		}
	}
}

func (pd *ProductDTAP) getToDoGrantsObjectsWriteRole() iter.Seq[Grant] {
	return func(yield func(Grant) bool) {
		for db, dbObjs := range pd.Interface.aggAccountObjects.DBs {
			for schema, schemaObjs := range dbObjs.Schemas {
				prvs := []PrivilegeComplete{}
				for _, p := range [2]PrivilegeComplete{
					PrivilegeComplete{Privilege: PrvCreate, CreateObjectType: ObjTpTable},
					PrivilegeComplete{Privilege: PrvCreate, CreateObjectType: ObjTpView},
				} {
					if !schemaObjs.hasGrantTo(ModeWrite, p) {
						prvs = append(prvs, p)
					}
				}
				if len(prvs) > 0 {
					if !yield(Grant{
						Privileges:    prvs,
						GrantedOn:     ObjTpSchema,
						Database:      db,
						Schema:        schema,
						GrantedTo:     ObjTpRole,
						GrantedToName: pd.WriteRole.ID,
					}) {
						return
					}
				}
			}
		}
	}
}

func (pd *ProductDTAP) getToDoGrantsOfWriteRoleToUserManagedRoles(semCnf *semantics.Config, cnf *Config) iter.Seq[Grant] {
	return func(yield func(Grant) bool) {
		for r := range pd.userManagedOwnersOfObjects {
			if _, ok := pd.writeRoleGrantedToUserManagedRoles[r]; !ok {
				if !yield(Grant{
					Privileges:    []PrivilegeComplete{PrivilegeComplete{Privilege: PrvUsage}},
					GrantedOn:     ObjTpRole,
					GrantedRole:   pd.WriteRole.ID,
					GrantedTo:     ObjTpRole,
					GrantedToName: r,
				}) {
					return
				}
			}
		}
	}
}

func (pd *ProductDTAP) getToDoOwnershipGrants() iter.Seq[Grant] {
	return func(yield func(Grant) bool) {
		for db, dbObjs := range pd.Interface.aggAccountObjects.DBs {
			for schema, schemaObjs := range dbObjs.Schemas {
				for obj, objAttr := range schemaObjs.Objects {
					if !objAttr.isOwnedByProductWriteRole {
						if !yield(Grant{
							Privileges:    []PrivilegeComplete{PrivilegeComplete{Privilege: PrvOwnership}},
							GrantedOn:     objAttr.ObjectType,
							Database:      db,
							Schema:        schema,
							Object:        obj,
							GrantedTo:     ObjTpRole,
							GrantedToName: pd.WriteRole.ID,
						}) {
							return
						}
					}
				}
			}
		}
	}
}

func (pd *ProductDTAP) getToDoFutureGrantsToDBRoles() iter.Seq[FutureGrant] {
	return func(yield func(FutureGrant) bool) {
		if !pd.Interface.pushToDoFutureGrants(yield) {
			return
		}
		for _, i := range pd.Interfaces {
			if !i.pushToDoFutureGrants(yield) {
				return
			}
		}
	}
}

func (pd *ProductDTAP) getToDoGrantsToDBRoles() iter.Seq[Grant] {
	return func(yield func(Grant) bool) {
		if !pd.Interface.pushToDoGrants(yield) {
			return
		}
		for _, i := range pd.Interfaces {
			if !i.pushToDoGrants(yield) {
				return
			}
		}
	}
}

func (pd *ProductDTAP) getTransferOwnershipGrants(newOwner semantics.Ident) iter.Seq[Grant] {
	return func(yield func(Grant) bool) {
		for _, g := range pd.toTransferOwnership {
			g.GrantedToName = newOwner
			if !yield(g) {
				return
			}
		}
	}
}

func (pd *ProductDTAP) getToDoFutureRevokesFromDBRoles() iter.Seq[FutureGrant] {
	return func(yield func(FutureGrant) bool) {
		if !pd.Interface.pushToDoFutureRevokes(yield) {
			return
		}
		for _, i := range pd.Interfaces {
			if !i.pushToDoFutureRevokes(yield) {
				return
			}
		}
	}
}

func (pd *ProductDTAP) getToDoRevokesFromDBRoles() iter.Seq[Grant] {
	return func(yield func(Grant) bool) {
		if !pd.Interface.pushToDoRevokes(yield) {
			return
		}
		for _, i := range pd.Interfaces {
			if !i.pushToDoRevokes(yield) {
				return
			}
		}
	}
}

func (pd *ProductDTAP) pushObjectCounts(yield func(ObjCountsRow) bool) bool {
	if !pd.Interface.pushObjectCounts(yield, pd.ProductDTAPID, "") {
		return false
	}
	for iid, i := range pd.Interfaces {
		if !i.pushObjectCounts(yield, pd.ProductDTAPID, iid) {
			return false
		}
	}
	return true
}
