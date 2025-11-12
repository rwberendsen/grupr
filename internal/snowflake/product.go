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
	pSNow.Interfaces = map[string]Interface{} // initialize / reset
	matched, err := newMatched(ctx, pSem.ObjectMatcher, c)
	if err != nil {
		// TODO: retry once? in case e.g., between we queried schemas in a DB, and objects in a schema, that schema was dropped
		// This could forseeably happen, and we might just want to try matching again in that case, once or even twice.
		return err // the result of returning a non nil error will be that all product refreshes are cancelled by the errgroup.Group
	}
	pSnow.Matched = matched
	for k, v := range pSem.Interfaces {
		// TODO: consider matching against already matched object in prod; faster, and less complex, not having to deal with fluid accountcache
		// Also, that would mean no errors in this section; just makes sense.
		// Finally, because all of this is fast and in memory, probably no need to respond to context cancellation
		pSnow.Interfaces[k] = newInterface(v, pSnow.Matched)
	}
	// TODO: query grants to (database) roles associated with product; do this only once, we don't expect it to be fluid, as 
	// grupr should be the only utiilty manipulating the privileges in gruprs scope on roles grupr manages. So, if already
	// initialized, then no need to query grants again on subsequent invocations of refreshProduct
	return nil
}
