package snowflake

import (
	"github.com/rwberendsen/grupr/internal/semantics"
	"gopkg.in/yaml.v3"
)

type GrupinDiff struct {
	Created map[string]Product
	Deleted map[string]bool
	Updated map[string]ProductDiff
}

func NewGrupinDiff(g semantics.GrupinDiff) GrupinDiff {
	r := GrupinDiff{map[string]Product{}, map[string]bool{}, map[string]ProductDiff{}}
	// walk over g, and enrich:
	// - created products and their interfaces with the exprs they consist of
	// - for updated products, both the old and new versions with the objects they consist of
	//
	// for deleted products we don't need to know the objects for now

	// as we match databases and schema's, we build up a local cache of the DB tree.
	c := newAccountCache()
	for k, v := range g.Created {
		r.Created[k] = newProduct(v, c)
	}
	for k := range g.Deleted {
		r.Deleted[k] = true
	}
	for k, v := range g.Updated {
		r.Updated[k] = newProductDiff(v, c)
	}
	return r
}

func (g GrupinDiff) String() string {
	data, err := yaml.Marshal(g)
	if err != nil {
		panic("GrupinDiff could not be marshalled")
	}
	return string(data)
}
