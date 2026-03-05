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
	"github.com/rwberendsen/grupr/internal/syntax"
	"github.com/rwberendsen/grupr/internal/util"
)

type ProductDTAP struct {
	semantics.ProductDTAPID
	IsProd bool
	*Interface
	Interfaces map[string]*Interface
	Consumes   map[syntax.InterfaceID]string // value is source dtap
	ReadRole   ProductRole
	WriteRole  ProductRole
	DeployedBy map[semantics.Ident]bool // initially set to false, then to true if GRANTS are found in Snowflake

	isReadRoleGrantedToWriteRole       bool
	writeRoleGrantedToUserManagedRoles map[semantics.Ident]struct{}
	refreshCount                       int // how many times has this ProductDTAP been refreshed: populated with Snowflake objects
	matchedAccountObjects              map[semantics.ObjExpr]*matchedAccountObjs
	revokeGrantsToReadRole             []Grant
	revokeFutureGrantsToWriteRole      []FutureGrant
	revokeGrantsToWriteRole            []Grant
	revokeRolesFromWriteRole           []Grant
	revokeGrantsOfWriteRoleToUsers     []Grant
	transferOwnership                  []Grant // ownership grants we no longer want based on the YAML
	isZombie                           bool
}

func NewProductDTAP(pdID semantics.ProductDTAPID, isProd bool, pSem semantics.Product, userGroupMappings map[string]semantics.UserGroupMapping,
	svcs map[string]semantics.ServiceAccount) *ProductDTAP {
	pd := &ProductDTAP{
		ProductDTAPID:         pdID,
		IsProd:                isProd,
		Interface:             NewInterface(pdID.DTAP, pSem.InterfaceMetadata, userGroupMappings[pSem.UserGroupMappingID]),
		Interfaces:            map[string]*Interface{},
		Consumes:              map[syntax.InterfaceID]string{},
		matchedAccountObjects: map[semantics.ObjExpr]*matchedAccountObjs{},
	}

	for id, iSem := range pSem.Interfaces {
		pd.Interfaces[id] = NewInterface(pd.DTAP, iSem, userGroupMappings[pSem.UserGroupMappingID])
	}

	for iid, dtapMapping := range pSem.Consumes {
		if sourceDTAP, ok := dtapMapping[pd.DTAP]; ok {
			pd.Consumes[iid] = sourceDTAP
		}
		// else, pd.DTAP must have been in a list of non consuming dtaps for this consumption spec, and we do not consume from pd.DTAP
	}

	for k := range pd.Interface.ObjectMatchers {
		pd.matchedAccountObjects[k] = &matchedAccountObjs{}
	}

	for _, svc := range svcs {
		if dtapMapping, ok := svc.Deploys[pd.ProductID]; ok {
			if svcDTAP, ok := dtapMapping[pd.DTAP]; ok {
				pd.DeployedBy[svc.Idents[svcDTAP]] = false // no GRANT found in Snowflake yet
			}
		}
	}

	return pd
}

func NewZombieProductDTAP(pdID semantics.ProductDTAPID) {
	return &ProductDTAP{
		ProductDTAPID: pdID,
		isZombie:      true,
	}
}

func (pd *ProductDTAP) refresh(ctx context.Context, synCnf *syntax.Config, cnf *Config, conn *sql.DB, c *accountCache) error {
	if err := pd.refresh_(ctx, synCnf, cnf, conn, c); err != nil {
		return err
	}
	pd.recalcObjects() // will reset all accountObjs
	// Reset other properties of pd that depend on which objects where matched
	pd.revokeFutureGrantsToWriteRole = []FutureGrant{}
	pd.revokeGrantsToWriteRole = []Grant{}
	pd.transferOwnership = []Grant
	return nil
}

func (pd *ProductDTAP) refresh_(ctx context.Context, synCnf *syntax.Config, cnf *Config, conn *sql.DB, c *accountCache) error {
	for {
		pd.refreshCount += 1
		if pd.refreshCount > cnf.MaxProductDTAPRefreshes {
			return fmt.Errorf("Max product refresh count reached")
		}
		util.SleepContext(ctx, 1<<pd.refreshCount-1) // exponential backoff
		if err := pd.refreshObjExprs(ctx, synCnf, cnf, conn, c); err != ErrObjectNotExistOrAuthorized {
			return err
		}
	}
}

func (pd *ProductDTAP) refreshObjExprs(ctx context.Context, synCnf *syntax.Config, cnf *Config, conn *sql.DB, c *accountCache) error {
	for e, om := range pd.ObjectMatchers {
		if err := c.match(ctx, synCnf, cnf, conn, om, pd.matchedAccountObjects[e]); err != nil {
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

func (pd *ProductDTAP) createProductRoles(ctx context.Context, synCnf *syntax.Config, cnf *Config,
	conn *sql.DB, productRoles map[ProductRole]struct{}) error {
	// Read role
	pd.ReadRole = newProductRole(synCnf, cnf, pd.ProductID, pd.DTAP, ModeRead)
	if _, ok := productRoles[pd.ReadRole]; !ok {
		if err := pd.ReadRole.Create(ctx, cnf, conn); err != nil {
			return err
		}
	}

	// Write role, identical logic, maybe refactor
	pd.WriteRole = newProductRole(synCnf, cnf, pd.ProductID, pd.DTAP, ModeWrite)
	if _, ok := productRoles[pd.WriteRole]; !ok {
		if err := pd.WriteRole.Create(ctx, cnf, conn); err != nil {
			return err
		}
	}

	return nil
}

func (pd *ProductDTAP) setWriteRoleGrantedToUserManagedRoles(ctx context.Context, cnf *Config, conn *sql.DB) error {
	pd.writeRoleGrantedToUserManagedRoles = map[semantics.Ident]struct{}{}
	for g, err := range QueryGrantsOfRoleToRoles(ctx, conn, pd.WriteRole.ID) {
		if err != nil {
			return err
		}
		if strings.HasPrefix(g.GrantedToName, cnf.ObjectPrefix) {
			return fmt.Errorf("product dtap write role '%s' granted to other grupr managed role, please take action to correct")
		}
		pd.writeRoleGrantedToUserManagedRoles[g.GrantedToName] = struct{}{}
	}
	return nil
}

func (pd *ProductDTAP) grant(ctx context.Context, synCnf *syntax.Config, cnf *Config, conn *sql.DB, productRoles map[ProductRole]struct{},
	grupinDisjointFromObject func(semantics.Ident, semantics.Ident, semantics.Ident) bool,
	c *accountCache) error {
	// Create the product roles if necessary (read, write)
	if err := pd.createProductRoles(ctx, synCnf, cnf, conn, productRoles); err != nil {
		return err
	}

	// Check (once) if we already granted the read role to the write role; if not, do it
	for grant, err := range QueryGrantsToRoleFiltered(ctx, cnf, conn, pd.WriteRole.ID, true, map[GrantTemplate]struct{}{
		GrantTemplate{
			PrivilegeComplete:           PrivilegeComplete{Privilege: PrvUsage},
			GrantedOn:                   ObjTpRole,
			GrantedRoleStartsWithPrefix: util.NewTrue(),
		}: {},
	}, nil) {
		if err != nil {
			return err
		}
		switch grant.Privilege {
		case PrvUsage:
			switch grant.GrantedOn {
			case ObjTpRole:
				// We may consider only querying for this relation once, and not after every refresh;
				// after all, this is a relationship between two roles that are managed by grupr itself.
				if grant.GrantedRole == pd.ReadRole.ID {
					pd.isReadRoleGrantedToWriteRole = true
				} else {
					pd.revokeRolesFromWriteRole = append(pd.revokeRolesFromWriteRole, grant)
					// Here we are silently assuming that since grant.GrantedRole starts with the prefix, it must
					// be a grupr managed product role. We might also check this assumption, and crash if it does
					// not hold, by trying to parse it like a product role.
				}
			}
			// Ignore; unmanaged grant (we might also panic, since we queried for managed grants only, so it would be unexpected if it did not match anything)
		}
	}

	if !pd.isReadRoleGrantedToWriteRole {
		DoGrants(ctx, cnf, conn, func(yield func(Grant) bool) {
			if !yield(Grant{
				PrivilegesComplete: []PrivilegeComplete{PrivilegeComplete{Privilege: PrvUsage}},
				GrantedOn:          ObjTpRole,
				GrantedRole:        pd.ReadRole.ID,
				GrantedTo:          ObjTpRole,
				GrantedToName:      pd.WriteRole.ID,
			}) {
				return
			}
		})
	}

	// Check (once) which roles currently have been granted the write role;
	// we do this prior to claiming ownership of objects, so that the user managed roles who
	// had ownership before on these objects do not lose ownership (this could cause downtime in workloads)
	// Instead, sysadmins and developers are expected to use the product write role for their workloads.
	// When everything works, they can then revoke the product write role from their user managed role
	if err := pd.setWriteRoleGrantedToUserManagedRoles(ctx, cnf, conn); err != nil {
		return err
	}

	// Now we handle grants on objects like databases, schemas, tables, and
	// views, that may be created or dropped concurrently
	// We retry granting all privileges on such objects a number until we
	// encounter something else than an error about objects we expect to
	// exist having been dropped concurrently;
	for {
		if err := pd.grant_(ctx, synCnf, cnf, conn, productRoles, grupinDisjointFromObject, c); err != ErrObjectNotExistOrAuthorized {
			return err
		}
	}
}

func (pd *ProductDTAP) setFutureGrantsToWriteRole(ctx context.Context, cnf *Config, conn *sql.DB) error {
	for g, err := range QueryFutureGrantsToRoleFiltered(ctx, conn, pd.WriteRole.ID, map[GrantTemplate]struct{}{
		GrantTemplate{
			PrivilegeComplete: PrivilegeComplete{Privilege: PrvCreate, CreateObjectType: ObjTpTable},
			GrantedOn:         ObjTpSchema,
		}: {},
		GrantTemplate{
			PrivilegeComplete: PrivilegeComplete{Privilege: PrvCreate, CreateObjectType: ObjTpView},
			GrantedOn:         ObjTpSchema,
		}: {},
		// Ignoring other grants, including future ownership grants. It would be quite annoying if such grants
		// were present, they would interfere with grupr (basically grupr would correct them if they grant
		// ownership to any other role than the product dtap role). But, grupr does not use future ownership
		// grants itself. Instead, the idea is that sysadmins would arrange for any service account that
		// deploys objects in this product dtap to assume the product role; when doing so, ownership is
		// already automatic.
	}, nil) {
		if err != nil {
			return err
		}

		switch g.GrantedIn {
		case ObjTpDatabase:
			switch g.GrantedOn {
			case ObjTpSchema:
				// Should we have this grant?
				if pd.Interface.ObjectMatchers.MatchAllSchemasInDB(g.Database) {
					// If yes, then, if we also have matched the object, mark on it that privilege on future objects was already granted
					if dbObjs, ok := pd.Interface.aggAccountObjects.DBs[g.Database]; ok {
						dbObjs.setFutureGrantTo(ModeWrite, g)
					}
				} else {
					// if not, then revoke this future grant
					//
					// Note that when we refreshed, we reset revokeFutureGrantsToWriteRole to the empty slice
					pd.revokeFutureGrantsToWriteRole = append(pd.revokeFutureGrantsToWriteRole, g)
				}
			}
			// Ignore this grant, it's not in grupr its scope (unmanaged grant)
		}
		// Ignore; unmanaged grant
	}
	return nil
}

func (pd *ProductDTAP) setGrantsToWriteRole(ctx context.Context, cnf *Config, conn *sql.DB,
	grupinDisjointFromObject func(semantics.Ident, semantics.Ident, semantics.Ident) bool) error {
	for g, err := range QueryGrantsToRoleFiltered(ctx, cnf, conn, pd.WriteRole.ID, true, map[GrantTemplate]struct{}{
		GrantTemplate{
			PrivilegeComplete: PrivilegeComplete{Privilege: PrvCreate, CreateObjectType: ObjTpTable},
			GrantedOn:         ObjTpSchema,
		}: {},
		GrantTemplate{
			PrivilegeComplete: PrivilegeComplete{Privilege: PrvCreate, CreateObjectType: ObjTpView},
			GrantedOn:         ObjTpSchema,
		}: {},
		GrantTemplate{
			PrivilegeComplete: PrivilegeComplete{Privilege: PrvOwnership},
			GrantedOn:         ObjTpTable,
		}: {},
		GrantTemplate{
			PrivilegeComplete: PrivilegeComplete{Privilege: PrvOwnership},
			GrantedOn:         ObjTpView,
		}: {},
	}, nil) {
		if err != nil {
			return err
		}
		switch g.Privileges[0].Privilege {
		case PrvCreate:
			switch g.CreateObjectType {
			case ObjTpTable, ObjTpView:
				if !pd.Interface.ObjectMatchers.DisjointFromSchema(g.Database, g.Schema) {
					if dbObjs, ok := pd.Interface.aggAccountObjs.DBs[g.Database]; ok {
						if schemaObjs, ok := dbObjs.Schemas[g.Schema]; ok {
							dbObjs.Schemas[g.Schema] = schemaObjs.setGrantTo(ModeWrite, g)
						}
					}
					// ignore, we did not match the object last time we refreshed, but the grant is fine, we leave it
				} else {
					// Note that when we refreshed, revokeGrantsToWriteRole was reset to an empty slice
					pd.revokeGrantsToWriteRole = append(pd.revokeGrantsToWriteRole, g)
				}
			}
			// Ignore; unmanaged grant
		case PrvOwnership:
			switch g.GrantedOn {
			case ObjTpTable, ObjTpView:
				if !pd.Interface.ObjectMatchers.DisjointFromObject(g.Database, g.Schema, g.Object) {
					if schemaObjs, ok := pd.Interface.aggAccountObjs.GetSchema(g.Database, g.Schema); ok {
						if aggObjAttr, ok := schemaObjs[g.Objects]; ok {
							schemaObjs[g.Object] = aggObjAttr.setGrantTo(ModeWrite, g)
						}
					}
				} else if grupinDisjointFromObject(g.Database, g.Schema, g.Object) {
					// There will be no other product claiming ownership of this object, we need to
					// transfer its ownership to a role that is not managed by grupr.
					// Note that when we refreshed, transferOwnership was reset to an empty slice
					pd.transferOwnership = append(pd.transferOwnership, g)
				}
			}
			// Ignore, unmanaged grant
		}
		// Ignore; unmanaged grant
	}
}

func (pd *ProductDTAP) getToDoGrantsOfWriteRoleToUserManagedRoles(cnf *Config) iter.Seq[Grant] {
	currentUserManagedRoleOwners := map[semantics.Ident]struct{}{}
	for _, dbObjs := range pd.Interface.aggAccountObjects.DBs {
		for _, schemaObjs := range dbObjs.Schemas {
			for _, aggObjAttr := range schemaObjs.Objects {
				if !strings.HasPrefix(aggObjAttr.Owner, cnf.ObjectPrefix) {
					if _, ok := pd.writeRoleGrantedToUserManagedRoles[aggObjAttr.Owner]; !ok {
						currentUserManagedRoleOwners[aggObjAttr.Owner] = struct{}{}
					}
				}
				// Else, this object belonged to a different grupr managed product write role before;
				// in this case, yeah, there will be downtime, we can't avoid it,
				// we are not going to enable the spaghetti you would get if we start granting
				// product write roles to each other.
			}
		}
	}
	return func(yield func(Grant) bool) {
		for r := range currentUserManagedRoleOwners {
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

func (pd *ProductDTAP) getToDoOwnershipGrants() iter.Seq[Grant] {
	return func(yield func(Grant) bool) {
		for db, dbObjs := range pd.Interface.aggAccountObjects.DBs {
			for schema, schemaObjs := range dbObjs.Schemas {
				for obj, objAttr := range schemaObjs.Objects {
					if !objAttr.isOwnedByWriteRole {
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

func (pd *ProductDTAP) grant_(ctx context.Context, synCnf *syntax.Config, cnf *Config, conn *sql.DB, productRoles map[ProductRole]struct{},
	grupinDisjointFromObject func(semantics.Ident, semantics.Ident, semantics.Ident) bool, c *accountCache) error {
	// First get the objects that are there in the account
	if err := pd.refresh(ctx, synCnf, cnf, conn, c); err != nil {
		return err
	}

	// Write grants go first, so that we do not have to copy all the read privileges we're about to set when granting ownership.
	// As with read grants, future grants go first
	if err := pd.setFutureGrantsToWriteRole(ctx, cnf, conn); err != nil {
		return err
	}
	if err := DoFutureGrants(ctx, cnf, conn, pd.getToDoFutureGrantsToWriteRole()); err != nil {
		return err
	}

	// Now, regular grants to the write role
	if err := pd.setGrantsToWriteRole(ctx, cnf, conn, grupinDisjointFromObject); err != nil {
		return err
	}
	if err := DoGrants(ctx, cnf, conn, pd.getToDoGrantsToWriteRole()); err != nil {
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
	if err := DoGrants(ctx, cnf, conn, pd.getToDoGrantsOfWriteRoleToUserManagedRoles()); err != nil {
		return err
	}
	// Then, make a second pass over the objects, and grant ownership to the write role.
	if err := DoGrantsIndividually(ctx, cnf, conn, pd.getToDoOwnershipGrants()); err != nil {
		return err
	}

	// Future grants next, so that as quickly as possible newly created objects will have correct privileges granted
	if err := pd.Interface.setFutureGrants(ctx, synCnf, cnf, conn, pd.ProductID, pd.DTAP, "", c); err != nil {
		return err
	}
	for iid, i := range pd.Interfaces {
		if err := i.setFutureGrants(ctx, synCnf, cnf, conn, pd.ProductID, pd.DTAP, iid, c); err != nil {
			return err
		}
	}
	if err := DoFutureGrants(ctx, cnf, conn, pd.getToDoFutureGrants()); err != nil {
		return err
	}

	// Now, regular grants
	if err := pd.Interface.setGrants(ctx, synCnf, cnf, conn, c); err != nil {
		return err
	}
	for _, i := range pd.Interfaces {
		if err := i.setGrants(ctx, synCnf, cnf, conn, c); err != nil {
			return err
		}
	}
	if err := DoGrants(ctx, cnf, conn, pd.getToDoGrants()); err != nil {
		return err
	}

	return nil
}

func (pd *ProductDTAP) getToDoFutureGrantsToWriteRole() iter.Seq[FutureGrant] {
	return func(yield func(FutureGrant) bool) {
		for db, dbObjs := range pd.Interface.aggAccountObjects.DBs {
			if dbObjs.MatchAllSchemas {
				prvs := []PrivilegeComplete{}
				for _, p := range [2]PrivilegeComplete{
					PrivilegeComplete{Privilege: PrvCreate, CreateObjectType: ObjTpTable},
					PrivilegeComplete{Privilege: PrvCreate, CreateObjectType: ObjTpView},
				} {
					if !dbObjs.hasFutureGrantTo(ModeWrite, p) {
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

func (pd *ProductDTAP) getToDoGrantsToWriteRole() iter.Seq[Grant] {
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

func (pd *ProductDTAP) getToDoFutureGrants() iter.Seq[FutureGrant] {
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

func (pd *ProductDTAP) getToDoGrants() iter.Seq[Grant] {
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

func (pd *ProductDTAP) getToDoDBRoleGrants(doProd bool, m map[semantics.ProductDTAPID]*ProductDTAP) iter.Seq[Grant] {
	return func(yield func(Grant) bool) {
		pd.pushToDoDBRoleGrants(yield, doProd, m)
	}
}

func (pd *ProductDTAP) pushToDoDBRoleGrants(yield func(Grant) bool, doProd bool, m map[semantics.ProductDTAPID]*ProductDTAP) bool {
	// First grant database roles of product-level interface role to product read role
	for db, dbObjs := range pd.Interface.aggAccountObjects.DBs {
		if !dbObjs.isUsageGrantedToRead {
			if !yield(Grant{
				Privileges:    []PrivilegeComplete{PrivilegeComplete{Privilege: PrvUsage}},
				GrantedOn:     ObjTpDatabaseRole,
				Database:      db,
				GrantedRole:   dbObjs.dbRole.Name,
				GrantedTo:     ObjTpRole,
				GrantedToName: pd.ReadRole.ID,
			}) {
				return false
			}
		}
	}
	// Next, grant database roles of interfaces to consumers (prod / non-prod)
	for _, i := range pd.Interfaces {
		if !i.pushToDoDBRoleGrants(yield, doProd, m) {
			return false
		}
	}
	return true
}

func (pd *ProductDTAP) pushToDoProductRoleGrants(yield func(Grant) bool) bool {
	for svc, alreadyGranted := range pd.DeployedBy {
		if !alreadyGranted {
			if !yield(Grant{
				Privileges:    []PrivilegeComplete{PrivilegeComplete{Privilege: PrvUsage}},
				GrantedOn:     ObjTpRole,
				GrantedRole:   pd.ReadRole.ID,
				GrantedTo:     ObjTpUser,
				GrantedToName: svc,
			}) {
				return false
			}
		}
	}
	return true
}

func (pd *ProductDTAP) setGrantedUsers(ctx context.Context, conn *sql.DB) error {
	for grant, err := range QueryGrantsOfRoleToUsers(ctx, conn, pd.WriteRole.ID) {
		if err != nil {
			return err
		}
		if _, ok := pd.DeployedBy[grant.GrantedToName]; ok {
			pd.DeployedBy[grant.GrantedToName] = true
		} else {
			pd.revokeGrantsOfWriteRoleToUsers = append(pd.revokeGrantsOfWriteRoleToUsers, grant)
		}
	}
	return nil
}

func (pd *ProductDTAP) revokeGrantToReadRole(g Grant) {
	pd.revokeGrantsToReadRole = append(pd.revokeGrantsToReadRole, g)
}

func (pd *ProductDTAP) getToDoFutureRevokes() iter.Seq[FutureGrant] {
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

func (pd *ProductDTAP) getToDoRevokes() iter.Seq[Grant] {
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

func (pd *ProductDTAP) revoke(ctx context.Context, synCnf *syntax.Config, cnf *Config, conn *sql.DB, productRoles map[ProductRole]struct{},
	grupinDisjointFromObject func(semantics.Ident, semantics.Ident, semantics.Ident) bool, c *accountCache) error {
	// We skip errors here, because users are not managed by grupr. And if users in the YAML
	// do not exist, well, then they would not have any roles granted to them, and we are done
	if err := DoRevokesSkipErrors(ctx, cnf, conn, slices.Values(pd.revokeGrantsOfWriteRoleToUsers)); err != nil {
		return err
	}

	// Only the read role should be granted to the write role; when it comes to grupr managed roles
	if err := DoRevokes(ctx, cnf, conn, slices.Values(pd.revokeRolesFromWriteRole)); err != nil {
		return err
	}

	// We skip errors here because this concerns relationships between
	// products, and refreshing brings no value on top of just rerunning
	// the whole program.  Note that an error would only mean some DB was
	// dropped concurrently, and this would mean the grants we thought we
	// needed to revoke would have already been dropped server side as
	// well.
	if err := DoRevokesSkipErrors(ctx, cnf, conn, slices.Values(pd.revokeGrantsToReadRole)); err != nil {
		return err
	}

	// Now we handle revoking of privileges on objects that may be concurrently dropped.
	// If during revoking of privileges on objects to interface database
	// roles we get ErrObjectNotExistOrAuthorized, we can refresh the
	// product and then first grant again, and then revoke;
	//
	// In this case, too, it means some objects were dropped, and thus any privileges
	// on them would have been dropped as well, rendering our job already done.
	// So why then refresh? Some reasons:
	// - There can be many objects, thousands. If whole schemas or even databases were dropped,
	//   concurrently, it could mean thousands of queries done in vain, taking a lot of time.
	// - Because there can be many objects, we do multiple statements
	//   per network call. But if we get an error in those, the batch is only partially
	//   applied. Figuring out which statement caused the error, removing
	//   it from the batch, and retrying  could be a repetitive exercise, although it
	//   may be quicker than refreshing the whole product.
	// - But, finally, objects being dropped concurrently signals activity in this data
	//   product while Grupr was running. This could mean also some objects were added that
	//   we would need to grant now. Rather than ignoring that signal and wait for the
	//   next entire Grupr run, it can save our DBAs some time if we act immediately and
	//   refresh just this single data product-dtap.
	err := pd.revoke_(ctx, cnf, conn)
	for err == ErrObjectNotExistOrAuthorized {
		err = pd.grant_(ctx, synCnf, cnf, conn, productRoles, grupinDisjointFromObject, c)
		if err != nil {
			continue
		}
		err = pd.revoke_(ctx, cnf, conn)
	}
	return err
}

func (pd *ProductDTAP) getTransferOwnershipGrants(newOwner semantics.Ident) iter.Seq[Grant] {
	return func(yield func(Grant) bool) {
		for _, g := range pd.transferOwnership {
			g.GrantedToName = newOwner
			if !yield(g) {
				return
			}
		}
	}
}

func (pd *ProductDTAP) revoke_(ctx context.Context, cnf *Config, conn *sql.DB) error {
	// We first revoke write privileges, to stop the wrong roles from creating objects asap
	// As with read privileges, we start with future privileges
	if err := DoFutureRevokes(ctx, cnf, conn, slices.Values(pd.revokeFutureGrantsToWriteRole)); err != nil {
		return err
	}
	if err := DoRevokes(ctx, cnf, conn, slices.Values(pd.revokeGrantsToWriteRole)); err != nil {
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
		pd.transferOwnership = []Grant{}
	}
	if !hasNewOwner && len(pd.transferOwnerhip) > 0 {
		log.Printf("WARN: multiple historic owners of objects that no longer should be owned by product '%s', dtap '%s', keeping ownership", pd.ProductID, pd.DTAP)
	}

	// Next, revoke read privileges
	// Future grants are revoked first, in case objects are being concurrently created, at least those
	// object will stop receiving incorrect grants first.
	if err := DoFutureRevokes(ctx, cnf, conn, pd.getToDoFutureRevokes()); err != nil {
		return err
	}
	if err := DoRevokes(ctx, cnf, conn, pd.getToDoRevokes()); err != nil {
		return err
	}
	return nil
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

func (pd *ProductDTAP) dropProductRolesIfZombie(ctx context.Context, cnf *Config, conn *sql.DB) error {
	if !pd.isZombie {
		return nil
	}
	if len(pd.transferOwnership) > 0 {
		log.Printf("WARN: product '%s', dtap '%s', has ownership of objects, not dropping product roles", pd.ProductID, pd.DTAP)
		return nil
	}
	if err := pd.ReadRole.Drop(ctx, cnf, conn); err != nil {
		return err
	}
	return pd.WriteRole.Drop(ctx, cnf, conn)
}
