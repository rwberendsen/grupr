package snowflake

import (
	"github.com/rwberendsen/grupr/internal/semantics"
)

type Product struct {
	Matched    Matched
	Interfaces map[string]Interface
}

func newProduct(p semantics.Product, c *accountCache) Product {
	// lazily reads which objects exist in Snowflake and adds them to c, modifying c
	r := Product{Interfaces: map[string]Interface{}}
	r.Matched = newMatched(p.ObjectMatcher, c)
	for k, v := range p.Interfaces {
		r.Interfaces[k] = newInterface(v, c)
	}
	return r
}
