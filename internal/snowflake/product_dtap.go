package snowflake

import (
	"context"
	"database/sql"
	"fmt"
	"iter"
	"slices"

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

	isReadGrantedToWrite       bool
	writeGrantedToRoles        map[semantics.Ident]struct{} // we grant the product write role to original owners of product objects
	refreshCount               int // how many times has this ProductDTAP been refreshed: populated with Snowflake objects
	matchedAccountObjects      map[semantics.ObjExpr]*matchedAccountObjs
	revokeGrantsToRead         []Grant
	revokeGrantsToWrite        []Grant
	revokeRolesFromWrite       []Grant
	revokeGrantsOfWriteToUsers []Grant
	transferOwnership          []Grant // ownership grants we no longer want based on the YAML
}

func NewProductDTAP(pdID semantics.ProductDTAPID, isProd bool, pSem semantics.Product, userGroupMappings map[string]semantics.UserGroupMapping,
	svcs map[string]semantics.ServiceAccount) *ProductDTAP {
	pd := &ProductDTAP{
		ProductDTAPID:         pdID,
		IsProd:                isProd,
		Interface:             NewInterface(pdID.DTAP, pSem.InterfaceMetadata, userGroupMappings[pSem.UserGroupMappingID]),
		Interfaces:            map[string]*Interface{},
		Consumes:              map[syntax.InterfaceID]string{},
		writeGrantedToRoles:   map[semantics.Ident]struct{}{},
		matchedAccountObjects: map[semantics.ObjExpr]*matchedAccountObjs{},
		revokeGrantsToRead:    []Grant{},
		revokeGrantsToWrite:   []Grant{},
		revokeRolesFromWrite:  []Grant{},
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

func (pd *ProductDTAP) refresh(ctx context.Context, synCnf *syntax.Config, cnf *Config, conn *sql.DB, c *accountCache) error {
	if err := pd.refresh_(ctx, synCnf, cnf, conn, c); err != nil {
		return err
	}
	pd.recalcObjects()
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

func (pd *ProductDTAP) grant(ctx context.Context, synCnf *syntax.Config, cnf *Config, conn *sql.DB, productRoles map[ProductRole]struct{},
	createDBRoleGrants map[semantics.Ident]struct{}, c *accountCache) error {
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
					pd.isReadGrantedToWrite = true
				} else {
					pd.revokeRolesFromWrite = append(pd.revokeRolesFromWrite, grant)
					// Here we are silently assuming that since grant.GrantedRole starts with the prefix, it must
					// be a grupr managed product role. We might also check this assumption, and crash if it does
					// not hold, by trying to parse it like a product role.
				}
			}
			// Ignore; unmanaged grant (we might also panic, since we queried for managed grants only, so it would be unexpected if it did not match anything)
		}
	}

	if !pd.isReadGrantedToWrite {
		DoGrants(ctx, cnf, conn, func(yield func(Grant) bool) {
			if !yield(Grant{
				PrivilegesComplete: []PrivilegeComplete{PrivilegeComplete{Privilege: PrvUsage}},
				GrantedOn: ObjTpRole,
				GrantedRole: pd.ReadRole.ID,
				GrantedTo: ObjTpRole,
				GrantedToName: pd.WriteRole.ID,
			}) {
				return
			}
		})
	}

	// Now we handle grants on objects like databases, schemas, tables, and views, that may be created or dropped concurrently
	// We retry granting all privileges on such objects correctly a number of times if we encounter errors due to objects we expect
	// to exist having been dropped concurrently
	for {
		if err := pd.grant_(ctx, synCnf, cnf, conn, productRoles, createDBRoleGrants, c); err != ErrObjectNotExistOrAuthorized {
			return err
		}
	}
}

func (pd *ProductDTAP) setGrantsToWrite(ctx context.Context, cnf *Config, conn *sql.DB, c *accountCache) error {
	// TODO: WIP: consider resetting all these properties, since this may be called multiple times
	// with the read grants, they were set on AggDBObjs, which are brand new objects after a refresh
	// but the ProductDTAP object itself is not refreshed. 
	// 
	// Though we could consider resetting the appropriate properties after a refresh, when a product is refreshed,
	// rather than here.
	for grant, err := range QueryGrantsToRoleFiltered(ctx, cnf, conn, pd.WriteRole.ID, true, map[GrantTemplate]struct{}{
		GrantTemplate{
			PrivilegeComplete:           PrivilegeComplete{Privilege: PrvCreate, CreateObjectType: ObjTpTable},
			GrantedOn:                   ObjTpSchema,
		}: {},
		GrantTemplate{
			PrivilegeComplete:           PrivilegeComplete{Privilege: PrvCreate, CreateObjectType: ObjTpView},
			GrantedOn:                   ObjTpSchema,
		}: {},
		GrantTemplate{
			PrivilegeComplete:           PrivilegeComplete{Privilege: PrvOwnership},
			GrantedOn:                   ObjTpTable,
		}: {},
		GrantTemplate{
			PrivilegeComplete:           PrivilegeComplete{Privilege: PrvOwnership},
			GrantedOn:                   ObjTpView,
		}: {},
	}, nil) {
		if err != nil {
			return err
		}
		switch grant.Privilege {
		case PrvCreate:
			switch grant.CreateObjectType {
			case ObjTpTable, ObjTpView:
				if !pd.Interface.ObjectMatchers.DisjointFromSchema(grant.Database, grant.Schema) {
					if !c.hasDB(grant.Database) {
						// between when we queried for grants and now the DB was dropped, and another product dtap grant has updated
						// the account cache with that information
						return ErrObjectNotExistOrAuthorized
					}
					if dbObjs, ok := pd.Interface.aggAccountObjs.DBs[grant.Database]; ok {
						if schemaObjs, ok := dbObjs.Schemas[grant.Schema]; ok {
							dbObjs.Schemas[grant.Schema] = schemaObjs.setGrantTo(ModeWrite, grant)
						}
					}
				} else {
					pd.revokeGrantsToWrite = append(pd.revokeGrantsToWrite, grant)
				}
			}
			// Ignore; unmanaged grant
		case PrvOwnership:
			switch grant.CreateObjectType {
			case ObjTpTable, ObjTpView:
				if !pd.Interface.ObjectMatchers.DisjointFromObject(grant.Database, grant.Schema, grant.Object) {
				} else {
					pd.transferOwnership = append(pd.transferOwnership, grant)
				}
			}
			// Ignore, unmanaged grant
		}
		// Ignore; unmanaged grant
	}
}

func (pd *ProductDTAP) grant_(ctx context.Context, synCnf *syntax.Config, cnf *Config, conn *sql.DB, productRoles map[ProductRole]struct{},
	createDBRoleGrants map[semantics.Ident]struct{}, c *accountCache) error {
	// First get the objects that are there in the account
	if err := pd.refresh(ctx, synCnf, cnf, conn, c); err != nil {
		return err
	}
	
	// Write grants go first, so that we do not have to copy all the read privileges we're about to set when granting ownership.
	// As with read grants, future grants go first
	if err := pd.setFutureGrantsToWrite(ctx, cnf, conn); err != nil {
		return err
	}
	if err := DoFutureGrantsToWrite(ctx, cnf, conn, pd.getToDoFutureGrantsToWrite()); err != nil {
		return err
	}

	// Now, regular grants to the write role
	if err := pd.setGrantsToWrite(ctx, cnf, conn); err != nil {
		return err
	}
	if err := DoGrants(ctx, cnf, conn, pd.getToDoGrantsToWrite()); err != nil {
		return err
	}
	// We do ownership separately; we don't do them in batches, cause they can take longer due to copying outbound grants;
	// they can even time-out for that reason, in which case we would want to retry them
	
	// Note that we grant objects directly to the product role, not via intermediate database roles; this is because we do
	// not want database roles showing up as grantor, it's just confusing; objects should have a single owning actual role.

	// Before we actually grant ownership to the write role, grant the write role itself to all the current owners of the objects
	// of interest. This way, they will not lose ownership, in fact, they will not lose any privilege, and running grupr will
	// not mess up any processes that may be running.

	// Therefore, first set which roles the product role was granted to; then find out which other roles it should be
	// granted to as well, based on the objects we matched; and grant these roles usage of the write role.
	if err := pd.setWriteGrantedToRoles(ctx, cnf, conn); err != nil {
		return err
	}
	if err := DoGrants(ctx, cnf, conn, pd.getToDoWriteGrantsToRoles()); err != nil {
		return err
	}
	// Then, make a second pass over the objects, and grant ownership to the write role.
	if err := DoGrantsRetry(ctx, cnf, conn, pd.getToDoOwnershipGrants()); err != nil {
		return err
	}

	// Future grants next, so that as quickly as possible newly created objects will have correct privileges granted
	if err := pd.Interface.setFutureGrants(ctx, synCnf, cnf, conn, createDBRoleGrants, pd.ProductID, pd.DTAP, "", c); err != nil {
		return err
	}
	for iid, i := range pd.Interfaces {
		if err := i.setFutureGrants(ctx, synCnf, cnf, conn, createDBRoleGrants, pd.ProductID, pd.DTAP, iid, c); err != nil {
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
			pd.revokeGrantsOfWriteToUsers = append(pd.revokeGrantsOfWriteToUsers, grant)
		}
	}
	return nil
}

func (pd *ProductDTAP) revokeGrantToRead(g Grant) {
	pd.revokeGrantsToRead = append(pd.revokeGrantsToRead, g)
}

func (pd *ProductDTAP) revokeGrantsToReadRole(ctx context.Context, cnf *Config, conn *sql.DB) error {
	// We skip errors here because this concerns relationships between
	// products, and refreshing brings no value on top of just rerunning
	// the whole program.  Note that an error would only mean some DB was
	// dropped concurrently, and this would mean the grants we thought we
	// needed to revoke would have already been dropped server side as
	// well.
	return DoRevokesSkipErrors(ctx, cnf, conn, slices.Values(pd.revokeGrantsToRead))
	// Note that if during revoking privileges on objects, errors were encountered,
	// and querying grants on objects was retried, then, at present, also these
	// revokes here are executed again. But, there is no harm in that, really.
}

func (pd *ProductDTAP) revokeWriteRoleFromUsers(ctx context.Context, cnf *Config, conn *sql.DB) error {
	// We skip errors here, because users are not managed by grupr. And if users in the YAML
	// do not exist, well, then they would not have any roles granted to them, and we are done
	return DoRevokesSkipErrors(ctx, cnf, conn, slices.Values(pd.revokeGrantsOfWriteToUsers))
	// Note that if during revoking privileges on objects, errors were encountered,
	// and querying grants on objects was retried, then, at present, also these
	// revokes here are executed again. But, there is no harm in that, really.
}

func (pd *ProductDTAP) refreshGrantRevoke(ctx context.Context, synCnf *syntax.Config, cnf *Config, conn *sql.DB, productRoles map[ProductRole]struct{},
	createDBRoleGrants map[semantics.Ident]struct{}, c *accountCache) error {
	// TODO WIP certainly not call grant(); maybe grant_(), or maybe just 
	// check again if you still hold the privileges you previously said you needed to revoke
	if err := pd.grant(ctx, synCnf, cnf, conn, productRoles, createDBRoleGrants, c); err != nil {
		return err
	}
	if err := pd.revoke(ctx, synCnf, cnf, conn, productRoles, createDBRoleGrants, c); err != nil {
		return err
	}
	return nil
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
	createDBRoleGrants map[semantics.Ident]struct{}, c *accountCache) error {
	if err := pd.revokeWriteRoleFromUsers(ctx, cnf, conn); err != nil {
		return err
	}
	if err := pd.revokeGrantsToReadRole(ctx, cnf, conn); err != nil {
		return err
	}

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


	// TODO WIP consider not granting again: merely checking if you still hold the grants
	// you said earlier you needed to revoke
	if err := pd.revoke_(ctx, cnf, conn); err == ErrObjectNotExistOrAuthorized {
		return pd.refreshGrantRevoke(ctx, synCnf, cnf, conn, productRoles, createDBRoleGrants, c)
	} else {
		return err
	}
}

func (pd *ProductDTAP) revoke_(ctx context.Context, cnf *Config, conn *sql.DB) error {
	// We first revoke write privileges, to stop the wrong roles from creating objects asap
	// As with read privileges, we start with future privileges
	if err := DoFutureRevokes(ctx, cnf, conn, pd.getToDoFutureRevokesToWrite()); err != nil {
		return err
	}
	if err := DoRevokes(ctx, cnf, conn, pd.getToDoRevokesToWrite()); err != nil {
		return err
	}
	// Now, we query again which privileges the write role has.
	// We only expect ownership grants to remain; we just revoked every other managed privilege.
	// But, since by now all product dtap threads have finished doing their grants, it could
	// be that they assumed ownership of some objects we owned before. In that case, no need
	// anymore to grant ownership to any other role.
	for grant, err := range QueryGrantsToRoleFiltered(ctx, cnf, conn, pd.WriteRole.ID, true, map[GrantTemplate]struct{}{
		GrantTemplate{
			PrivilegeComplete:           PrivilegeComplete{Privilege: PrvOwnership},
			GrantedOn:                   ObjTpTable,
		}: {},
		GrantTemplate{
			PrivilegeComplete:           PrivilegeComplete{Privilege: PrvOwnership},
			GrantedOn:                   ObjTpView,
		}: {},
	}, nil) {
	}

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
