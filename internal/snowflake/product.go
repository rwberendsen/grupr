package snowflake

import (
	"context"

	"github.com/rwberendsen/grupr/internal/semantics"
)

type Product struct {
	Matched    Matched
	Interfaces map[string]Interface
}

func newProduct(pSem semantics.Product) Product {
	p := &Product{}
	p.Matched = newMatched(pSem.ObjectMatchers)
	p.Interfaces = map[string]Interface{}
	for k, v := range pSem.Interfaces {
		p.Interfaces[k] = newInterface(v)
	}
}

func refreshProduct(ctx context.Context, conn *sql.DB,  pSem semantics.Product, pSnow *Product, c *accountCache) error {
	matched, err := newMatched(ctx, conn, pSem.ObjectMatcher, c)
	if err != nil {
		// TODO: retry once? in case e.g., between we queried schemas in a DB, and objects in a schema, that schema was dropped
		// This could forseeably happen, and we might just want to try matching again in that case, once or even twice.
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
