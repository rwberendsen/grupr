package snowflake

import (
	"context"

	"github.com/rwberendsen/grupr/internal/semantics"
	"github.com/rwberendsen/grupr/internal/util"
)

type ProductDTAP struct {
	semantics.ProductDTAPID
	IsProd bool
	*Interface
	Interfaces map[string]*Interface
	Consumes map[syntax.InterfaceID]string // value is source dtap
	ReadRole ProductRole

	refreshCount int // how many times has this ProductDTAP been refreshed: populated with Snowflake objects
	matchedAccountObjects map[semantics.ObjExpr]*matchedAccountObjs
	hasProductRoles bool
	revokeGrantsToRead map[Grant]struct{}
}

func NewProductDTAP(pID string, dtap string, isProd bool, pSem semantics.Product) *ProductDTAP {
	pd := &ProductDTAP{
		ProductDTAPID: ProductDTAPID{ProductID: pID, DTAP: dtap,},
		IsProd: isProd,
		Interface: NewInterface(dtap, pSem.InterfaceMetadata),
		Interfaces: map[string]Interface{},
		Consumes: map[syntac.InterfaceID]string{},
		matchedAccountObjects: map[semantics.ObjExpr]*matchedAccountObjs{},
		revokeGrantsToRead: map[Grant]struct{}{},
	}

	for id, iSem := range pSem.Interfaces {
		pd.Interfaces[id] = NewInterface(dtap, iSem)
	}

	for iid, dtapMapping := range pSem.Consumes {
		if sourceDTAP, ok := dtapMapping[pd.DTAP]; ok {
			pd.Consumes[iid] = sourceDTAP
		}
	}

	for k := range pd.Interface.ObjectMatchers {
		p.matchedAccountObjects[k] = &matchedAccountObjects{}
	}
	
	return pd
}

func (pd *ProductDTAP) refresh(ctx context.Context, cnf *Config, conn *sql.DB, c *accountCache) error {
	if err := pd.refresh_(ctx, cnf, conn, c); err != nil { return err }
	pd.recalcObjects()
}

func (pd *ProductDTAP) refresh_(ctx context.Context, cnf *Config, conn *sql.DB, c *accountCache) error {
	for {
		pd.refreshCount += 1
		if pd.refreshCount > cnf.MaxProductRefreshCount {
			return fmt.Errorf("Max product refresh count reached")
		}
		util.SleepContext(ctx, 1 << pd.refreshCount - 1) // exponential backoff
		if err := pd.refreshObjExprs(ctx, conn, c); err != ErrObjectNotExistOrAuthorized {
			return err
		}
	}
}

func (pd *ProductDTAP) refreshObjExprs(ctx context.Context, conn *sql.DB, c *accountCache) error {
	for e, om := range p.ObjectMatchers {
		if err := c.match(ctx, conn, om, pd.matchedAccountObjects[e]); err != nil {
			return err
		}
	}
	return nil
}

func (pd *ProductDTAP) recalcObjects() {
	pd.Interface.recalcObjectsFromMatched(pd.matchedAccountObjects)
	for _, v := range pd.Interfaces {
		v.recalcObjects(pd.accountObjects)
		v.agggregate() // this will free memory held by AccountObjs by ObjExpr
	}
	pd.Interface.agggregate() // we needed to hold on to AccountObjs by ObjExpr until we derived all interface objects
}

func (pd *ProductDTAP) createProductRoles(ctx context.Context, synCnf *syntax.Config, cnf *Config,
			      conn *sql.DB, productRoles map[ProductRole]struct{}) error {
	if pd.hasProductRoles { return nil }
	pd.ReadRole = newProductRole(synCnf, cnf, pd.ProductID, pd.DTAP, ModeRead)
	if _, ok := productRoles[productRole]; !ok {
		if err := productRole.create(ctx, cnf, conn); err != nil { return err }
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
	if err := pd.refresh(ctx, cnf, conn, c); err != nil { return err }
	if err := pd.createProductRoles(ctx, synCnf, cnf, conn, productRoles); err != nil { return err }

	// Future grants go first, so that as quickly as possible newly created objects will have correct privileges granted
	if err := pd.Interface.setFutureGrants(ctx, synCnf, cnf, conn, createDBRoleGrants, pd.ProductID, pd.DTAP, "", c); err != nil { return err }
	for iid, i := range pd.Interfaces {
		if err := i.setFutureGrants(ctx, synCnf, cnf, conn, createDBRoleGrants, pd.ProductID, pd.DTAP, iid, c); err != nil { return err }
	}
	if err := DoFutureGrants(ctx, cnf, conn, pd.getTodoFutureGrants()); err != nil { return err }

	// Now, regular grants
	if err := pd.Interface.setGrants(ctx, synCnf, cnf, conn, c); err != nil { return err }
	for iid, i := range pd.Interfaces {
		if err := i.setGrants(ctx, synCnf, cnf, conn, c); err != nil { return err }
	}
	if err := DoGrants(ctx, cnf, conn, pd.getToDoGrants()); err != nil { return err }

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

func (pd *ProductDTAP) pushToDoDBRoleGrants(yield func(Grant) bool, doProd bool, isProd func(ProductDTAP) bool) bool {
	// First grant database roles of product-level interface role to product read role
	for db, dbObjs := range pd.Interface.aggAccountObjects.DBs {
		if !dbObjs.isUsageGrantedToRead {
			if !yield(Grant{
				Privilege: PrvUsage,
				GrantedOn: ObjTpDatabaseRole,
				Database: db,
				GrantedRole: dbObjs.dbRole,
				GrantedTo: ObjTpRole,
				GrantedToRole: pd.ReadRole,
			}) {
				return false
			}
		}
	}
	// Next, grant database roles of interfaces to consumers (prod / non-prod)
	for _, i := range pd.Interfaces {
		if !i.pushToDoDBRoleGrants(yield, doProd, isProd) {
			return false
		}
	}
	return true
}

func (p *ProductDTAP) revoke(ctx context.Context, cnf *Config, conn *sql.DB) error {
	// if during granting we get ErrObjectNotExistOrAuthorized, we should refresh the product and then first grant
	// again, and then revoke
	if err := p.Interface.revoke(ctx, cnf, conn, databaseRoles); err != nil { return err }
	for iid, i := range p.Interfaces {
		if err := i.revoke(ctx, cnf, conn, databaseRoles); err != nil { return err }
	}
	return nil
}
