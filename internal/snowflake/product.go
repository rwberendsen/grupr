package snowflake

import (
	"context"

	"github.com/rwberendsen/grupr/internal/semantics"
	"github.com/rwberendsen/grupr/internal/util"
)

type Product struct {
	Interface
	Interfaces map[string]*Interface
	pSem semantics.Product
	refreshCount int
	matchedAccountObjects map[semantics.ObjExpr]*matchedAccountObjs
}

func newProduct(pSem semantics.Product) *Product {
	p := &Product{pSem: pSem}
	p.matchedAccountObjects = map[semantics.ObjExpr]*AccountObjs{}
	for k := range pSem.ObjectMatchers {
		p.matchedAccountObjects[k] = &matchedAccountObjects{}
	}
	return p
}

func (p *Product) refresh(ctx context.Context, cnf *Config, conn *sql.DB, c *accountCache) error {
	err := p.refreshRecur(ctx, cnf, conn, c)
	if err != nil { return err }
	p.calcObjects()
}

func (p *Product) refreshRecur(ctx context.Context, cnf *Config, conn *sql.DB, c *accountCache) error {
	p.refreshCount += 1
	if p.refreshCount > cnf.MaxProductRefreshCount {
		return fmt.Errorf("Max product refresh count reached")
	}
	util.SleepContext(ctx, 1 << p.refreshCount - 1) // exponential backoff
	for err := p.refreshObjExprs(ctx, conn, c); err != nil {
		if err != ErrObjectNotExistOrAuthorized {
			return err
		}
		err = p.refreshRecur(ctx, cnf, conn, c) 
	}
	return nil
}

func (p *Product) refreshObjExprs(ctx context.Context, conn *sql.DB, c *accountCache) error {
	for e, _ := range p.pSem.ObjectMatchers {
		if err := c.match(ctx, conn, e, p.matchedAccountObjects[e]); err != nil {
			return err
		}
	}
	return nil
}

func (p *Product) calcObjects() {
	p.Interface = *newInterfaceFromMatched(p.MatchedAccountObjects, p.pSem.ObjectMatchers)
	p.Interfaces = map[string]*Interface{}
	for k, v := range p.pSem.Interfaces {
		p.Interfaces[k] = newInterface(p.AccountObjects, v.ObjectMatchers)
	}
}

func (p *Product) grant (ctx context.Context, cnf *Config, conn *sql.DB, databaseRoles map[string]bool) error {
	// if during granting we get ErrObjectNotExistOrAuthorized, we should refresh the product and try again
	if err := p.Interface.grant(ctx, cnf, conn, databaseRoles); err != nil { return err }
	for iid, i := range p.Interfaces {
		if err := i.grant(ctx, cnf, conn, databaseRoles); err != nil { return err }
	}
	return nil
}

func (p *Product) revoke(ctx context.Context, cnf *Config, conn *sql.DB) error {
	// if during granting we get ErrObjectNotExistOrAuthorized, we should refresh the product and then first grant
	// again, and then revoke
	if err := p.Interface.revoke(ctx, cnf, conn, databaseRoles); err != nil { return err }
	for iid, i := range p.Interfaces {
		if err := i.revoke(ctx, cnf, conn, databaseRoles); err != nil { return err }
	}
	return nil
}

func (pSnow *Product) tableCount() int {
	// todo for basic stats, that one also needs some rewriting
	return 0
}
