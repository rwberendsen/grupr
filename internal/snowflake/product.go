package snowflake

import (
	"context"

	"github.com/rwberendsen/grupr/internal/semantics"
	"github.com/rwberendsen/grupr/internal/util"
)

type Product struct {
	refreshCount int
	Matched    Matched
	Interfaces map[string]Interface
}

func newProduct(pSem semantics.Product) *Product {
	p := &Product{}
	p.Matched = newMatched(pSem.ObjectMatchers)
	p.Interfaces = map[string]Interface{}
	for k, v := range pSem.Interfaces {
		p.Interfaces[k] = newInterface(v)
	}
}

func (pSnow *Product) refresh(ctx context.Context, cnf *Config, conn *sql.DB,  pSem semantics.Product, c *accountCache) error {
	defer refreshCount += 1
	if p.refreshCount == cnf.MaxProductRefreshCount {
		return fmt.Errorf("Max product refresh count reached")
	}
	util.SleepContext(ctx, 1 << p.refreshCount - 1) // exponential backoff
	
	for err := p.Matched.refresh(ctx, conn, pSem.ObjectMatcher, c); err != nil {
	}
	if err != nil {
		if err == ErrObjectNotExistOrAuthorized {
			// During work objects may have been dropped; retry
			err := pSnow.refresh(ctx, cnf, conn, pSem, c)
		}
		return err // the result of returning a non nil error will be that all product refreshes are cancelled by the errgroup.Group
	}
	// Okay, so reaching out to the database went well, the rest is just a matter of some in-memory data structure walking,
	// we can start to update pSnow
	pSnow.Matched = matched
	pSNow.Interfaces = map[string]Interface{} // initialize / reset
	for k, v := range pSem.Interfaces {
		pSnow.Interfaces[k] = newInterface(v, pSnow.Matched)
	}
	// TODO: query grants to (database) roles associated with product; do this only once, we don't expect it to be fluid, as 
	// grupr should be the only utiilty manipulating the privileges in gruprs scope on roles grupr manages. So, if already
	// initialized, then no need to query grants again on subsequent invocations of refreshProduct
	return nil
}

func (pSnow *Product) grant(ctx context.Context, cnf *Config, conn *sql.DB, pSem semantics.Product, c *accountCache) error {
	// if during granting we get ErrObjectNotExistOrAuthorized, we should refresh the product and try again
}

func (pSnow *Product) revoke(ctx context.Context, cnf *Config, conn *sql.DB, pSem semantics.Product, c *accountCache) error {
}
