package snowflake

import (
	"context"

	"github.com/rwberendsen/grupr/internal/semantics"
	"github.com/rwberendsen/grupr/internal/util"
)

type Product struct {
	AccountObjects map[semantics.ObjExpr]*AccountObjs
	Interfaces map[string]Interface
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
	for e, _ := range pSem.ObjectMatchers {
		if err := c.match(ctx, conn, e, p.matchedAccountObjects[e]); err != nil {
			return err
		}
	}
	return nil
}

func (p *Product) calcObjects() {
	p.AccountObjects = map[semantics.ObjExpr]*AccountObjects{}
	for 
}


func (pSnow *Product) grant(ctx context.Context, cnf *Config, conn *sql.DB, pSem semantics.Product, c *accountCache) error {
	// if during granting we get ErrObjectNotExistOrAuthorized, we should refresh the product and try again

	// only during doing something like granting will we compute stuff like objects in Interfaces, and what should
	// be excluded.
	// WIP ...
	interfaces = map[string]Interface{} // initialize / reset
	for k, v := range pSem.Interfaces {
		pSnow.Interfaces[k] = newInterface(v, pSnow.Matched)
	}
}

func (pSnow *Product) revoke(ctx context.Context, cnf *Config, conn *sql.DB, pSem semantics.Product, c *accountCache) error {
	// if during granting we get ErrObjectNotExistOrAuthorized, we should refresh the product and then first grant
	// again, and then revoke
}

func (pSnow *Product) tableCount() int {
	// todo for basic stats, that one also needs some rewriting
}
