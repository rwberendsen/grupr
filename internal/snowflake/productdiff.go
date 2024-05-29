package snowflake

import (
	"github.com/rwberendsen/grupr/internal/semantics"
)

type ProductDiff struct {
	Old Product
	New Product
}

func newProductDiff(d semantics.ProductDiff, *accountCache c) ProductDiff {
	// lazily reads which objects exist in Snowflake and adds them to c, modifying c
	return ProductDiff{newProduct(d.Old, c), newProduct(d.New, c)}
}