package snowflake

import (
	"context"

	"github.com/rwberendsen/grupr/internal/semantics"
)

type Product struct {
	Matched    Matched
	Interfaces map[string]Interface
}

func refreshProduct(ctx context.Context, pSem semantics.Product, pSnow *Product, c *accountCache) error {
	r.Matched = newMatched(p.ObjectMatcher, c)
	for k, v := range p.Interfaces {
		// TODO: consider matching against already matched object in prod; faster, and less complex, not having to deal with fluid accountcache
		r.Interfaces[k] = newInterface(v, c)
	}
	return r
	// TODO: even while creating this product, just collecting the objects, we may encounter errors, e.g., we match a schema, but when we a bit later want to list objects in it, it has been dropped.
	// what to do in that case? This method does not even return an error
}

// TODO: conider method like refreshProduct, in case a product thread encountered an error like tables that were dropped while grupr ran and it tried to grant select on those objects; 
// the refresh product method would just loop over everything again, collecting all objects again from the accountcache, after which running the grants could be retried.
