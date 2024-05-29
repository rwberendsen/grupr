package snowflake

import (
	"github.com/rwberendsen/grupr/internal/semantics"
)

type GrupsDiff struct {
	Created map[string]Product
	Deleted map[string]bool
	Updated map[string]ProductDiff
}

func NewGrupsDiff(g semantics.GrupsDiff) GrupsDiff {
	r := GrupsDiff{map[string]Product{}, map[string]Product{}, map[string]ProductDiff{}}
	// walk over g, and enrich:
	// - created products and their interfaces with the exprs they consist of
	// - for updated products, both the old and new versions with the objects they consist of
	//
	// for deleted products we don't need to know the objects for now

	// as we match databases and schema's, we build up a local cache of the DB tree.
	c := &accountCache{map[string]*dbCache{}, map[string]bool{}}
	for k, v := range g.Created {
		r.Created[k] = newProduct(v, c)
	}
	for k, _ := range g.Deleted {
		r.Deleted[k] = true
	}
	for k, v := range g.Updated {
		r.Updated[k] = newProductDiff(v, c)
	}
	return r
}
