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

	isReadRoleNew         bool
	refreshCount          int // how many times has this ProductDTAP been refreshed: populated with Snowflake objects
	matchedAccountObjects map[semantics.ObjExpr]*matchedAccountObjs
	hasProductRoles       bool
	revokeGrantsToRead    []Grant
}

func NewProductDTAP(pdID semantics.ProductDTAPID, isProd bool, pSem semantics.Product, userGroupMappings map[string]semantics.UserGroupMapping) *ProductDTAP {
	pd := &ProductDTAP{
		ProductDTAPID:         pdID,
		IsProd:                isProd,
		Interface:             NewInterface(pdID.DTAP, pSem.InterfaceMetadata, userGroupMappings[pSem.UserGroupMappingID]),
		Interfaces:            map[string]*Interface{},
		Consumes:              map[syntax.InterfaceID]string{},
		matchedAccountObjects: map[semantics.ObjExpr]*matchedAccountObjs{},
		revokeGrantsToRead:    []Grant{},
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
	if pd.hasProductRoles {
		pd.isReadRoleNew = false // in the mean-time, it may have acquired grants, and it is no longer correct to assume it would have none.
		return nil
	}
	pd.ReadRole = newProductRole(synCnf, cnf, pd.ProductID, pd.DTAP, ModeRead)
	if _, ok := productRoles[pd.ReadRole]; !ok {
		if err := pd.ReadRole.Create(ctx, cnf, conn); err != nil {
			return err
		}
		pd.isReadRoleNew = true
	}
	pd.hasProductRoles = true
	return nil

}

func (pd *ProductDTAP) grant(ctx context.Context, synCnf *syntax.Config, cnf *Config, conn *sql.DB, productRoles map[ProductRole]struct{},
	createDBRoleGrants map[string]struct{}, c *accountCache) error {
	for {
		if err := pd.grant_(ctx, synCnf, cnf, conn, productRoles, createDBRoleGrants, c); err != ErrObjectNotExistOrAuthorized {
			return err
		}
	}
}

func (pd *ProductDTAP) grant_(ctx context.Context, synCnf *syntax.Config, cnf *Config, conn *sql.DB, productRoles map[ProductRole]struct{},
	createDBRoleGrants map[string]struct{}, c *accountCache) error {
	if err := pd.refresh(ctx, synCnf, cnf, conn, c); err != nil {
		return err
	}
	if err := pd.createProductRoles(ctx, synCnf, cnf, conn, productRoles); err != nil {
		return err
	}

	// Future grants go first, so that as quickly as possible newly created objects will have correct privileges granted
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
				GrantedToRole: pd.ReadRole.ID,
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

func (pd *ProductDTAP) revokeGrantFromRead(g Grant) {
	pd.revokeGrantsToRead = append(pd.revokeGrantsToRead, g)
}

func (pd *ProductDTAP) revokeFromProductRole(ctx context.Context, cnf *Config, conn *sql.DB) error {
	// We skip errors here because this concerns relationships between
	// products, and refreshing brings no value on top of just rerunning
	// the whole program.  Note that an error would only mean some DB was
	// dropped concurrently, and this would mean the grants we thought we
	// needed to revoke would have already been dropped server side as
	// well.
	return DoRevokesSkipErrors(ctx, cnf, conn, slices.Values(pd.revokeGrantsToRead))
}

func (pd *ProductDTAP) refreshGrantRevoke(ctx context.Context, synCnf *syntax.Config, cnf *Config, conn *sql.DB, productRoles map[ProductRole]struct{},
	createDBRoleGrants map[string]struct{}, c *accountCache) error {
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
	createDBRoleGrants map[string]struct{}, c *accountCache) error {
	if err := pd.revokeFromProductRole(ctx, cnf, conn); err != nil {
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
	if err := pd.revoke_(ctx, cnf, conn); err == ErrObjectNotExistOrAuthorized {
		return pd.refreshGrantRevoke(ctx, synCnf, cnf, conn, productRoles, createDBRoleGrants, c)
	} else {
		return err
	}
}

func (pd *ProductDTAP) revoke_(ctx context.Context, cnf *Config, conn *sql.DB) error {
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

func (pd *ProductDTAP) pushObjectCounts(yield func(ObjCountsRow) bool, pdID semantics.ProductDTAPID) bool {
	if !pd.Interface.pushObjectCounts(yield, pdID, "") {
		return false
	}
	for iid, i := range pd.Interfaces {
		if !i.pushObjectCounts(yield, pdID, iid) {
			return false
		}
	}
	return true
}
