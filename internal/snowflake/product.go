package snowflake

import (
	"context"

	"github.com/rwberendsen/grupr/internal/semantics"
	"github.com/rwberendsen/grupr/internal/util"
)

type Product struct {
	refreshCount int
	matchedAccountObjects map[semantics.ObjExpr]*matchedAccountObjs
	AccountObjects map[semantics.ObjExpr]*AccountObjs
}

func newProduct(pSem semantics.Product) *Product {
	p := &Product{}
	p.AccountObjects = map[semantics.ObjExpr]*AccountObjs{}
	for k := range pSem.ObjectMatchers {
		p.AccountObjects[k] = &AccountObjects{}
	}
}

func (p *Product) refresh(ctx context.Context, cnf *Config, conn *sql.DB, c *accountCache) error {
	defer refreshCount += 1
	if p.refreshCount == cnf.MaxProductRefreshCount {
		return fmt.Errorf("Max product refresh count reached")
	}
	util.SleepContext(ctx, 1 << p.refreshCount - 1) // exponential backoff
	for err := p.refreshObjExprs(ctx, conn, c); err != nil {
		if err != ErrObjectNotExistOrAuthorized {
			return err
		}
		err = p.refresh(ctx, cnf, conn, c) 
	}
	return nil
}

func (p *Product) refreshObjExprs(ctx context.Context, conn *sql.DB, c *accountCache) error {
	for k, v := range p.AccountObjects {
		c.match() // WIP
	}
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
