package snowflake

import (
	"log"

	"github.com/rwberendsen/grupr/internal/semantics"
	"gopkg.in/yaml.v3"
)

type Grups struct {
	Products map[string]Product
}

func NewGrups(g semantics.Grups) Grups {
	r := Grups{map[string]Product{}}
	c := newAccountCache()
	for k, v := range g.Products {
		r.Products[k] = newProduct(v, c)
	}
	log.Printf("accountCache: %v", *c)
	return r
}

func (g Grups) String() string {
	data, err := yaml.Marshal(g)
	if err != nil {
		panic("grups could not be marshalled")
	}
	return string(data)
}
