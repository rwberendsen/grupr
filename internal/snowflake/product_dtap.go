package snowflake

import (
	"context"

	"github.com/rwberendsen/grupr/internal/semantics"
	"github.com/rwberendsen/grupr/internal/util"
)

type ProductDTAP struct {
	ProductID string
	DTAP string
	IsProd bool
	Interface
	Interfaces map[string]Interface
	Consumes map[syntax.InterfaceID]string // dtap

	refreshCount int // how many times has this ProductDTAP been refreshed: populated with Snowflake objects
	matchedAccountObjects map[semantics.ObjExpr]*matchedAccountObjs
}

func NewProductDTAP(pID string, dtap string, isProd bool, pSem semantics.Product) *ProductDTAP {
	pd := &ProductDTAP{
		ProductID: pID,
		DTAP: dtap,
		IsProd: isProd,
		Interface: NewInterface(dtap, pSem.ObjectMatchers),
		Interfaces: map[string]Interface{},
		Consumes: map[syntac.InterfaceID]string{},
	}

	for id, iSem := range pSem.Interfaces {
		pd.Interfaces[id] = NewInterface(dtap, iSem.ObjectMatchers)		
	}

	for iid, dtapMapping := range pSem.Consumes {
		if pd.IsProd {
			pd.Consumes[iid] = ""
		} else {
			pd.Consumes[iid] = dtapMapping[pd.DTAP]
		}
	}

	

	// WIP
	p := &Product{pSem: pSem}
	p.matchedAccountObjects = map[semantics.ObjExpr]*AccountObjs{}
	for k := range pSem.ObjectMatchers {
		p.matchedAccountObjects[k] = &matchedAccountObjects{}
	}
	return p
}

func (pd *ProductDTAP) refresh(ctx context.Context, cnf *Config, conn *sql.DB, c *accountCache) error {
	if err := pd.refresh_(ctx, cnf, conn, c); err != nil { return err }
	pd.calcObjects()
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
	for e, om := range p.pSem.ObjectMatchers {
		if err := c.match(ctx, conn, om, p.matchedAccountObjects[e]); err != nil {
			return err
		}
	}
	return nil
}

func (p *ProductDTAP) calcObjects() {
	p.Interface = *newInterfaceFromMatched(p.MatchedAccountObjects, p.pSem.ObjectMatchers)
	p.Interfaces = map[string]*Interface{}
	for k, v := range p.pSem.Interfaces {
		p.Interfaces[k] = newInterface(p.AccountObjects, v.ObjectMatchers)
	}
}

func (p *ProductDTAP) createRoles(ctx context.Context, synCnf *syntax.Config, cnf *Config,
			      conn *sql.DB, productRoles map[ProductRole]struct{}) error {
	for dtap := range p.pSem.DTAPs.All() {
		for mode := range cnf.Modes {
			productRole := newProductRole(synCnf, cnf, p.pSem.ID, dtap, mode)
			if _, ok := productRoles[productRole]; !ok {
				productRole.create(ctx, cnf, conn)	
			}
		}
	}
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
	if err := pd.createRoles(ctx, synCnf, cnf, conn, productRoles); err != nil { return err }
	if err := pd.Interface.grant(ctx, synCnf, cnf, conn, createDBRoleGrants, pd.pSem.DTAPs, pd.pSem.ID, "", pd.pSem.ObjectMatchers, c); err != nil { return err }
	for iid, i := range pd.Interfaces {
		if err := i.grant(ctx, synCnf, cnf, conn, createDBRoleGrants, pd.DTAPs, pd.pSem.ID, iid, pd.pSem.Interfaces[iid].ObjectMatchers, c); err != nil { return err }
	}
	return nil
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

func (pSnow *ProductDTAP) tableCount() int {
	// todo for basic stats, that one also needs some rewriting
	return 0
}
