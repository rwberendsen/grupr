package snowflake

import (
	"github.com/rwberendsen/grupr/internal/semantics"
	"gopkg.in/yaml.v3"
)

type Grupin struct {
	Products map[string]Product
}

func NewGrupin(g semantics.Grupin) Grupin {
	r := Grupin{map[string]Product{}}
	c := newAccountCache()
	for k, v := range g.Products {
		r.Products[k] = newProduct(v, c)
	}
	return r
}

func (g Grupin) String() string {
	data, err := yaml.Marshal(g)
	if err != nil {
		panic("grups could not be marshalled")
	}
	return string(data)
}
