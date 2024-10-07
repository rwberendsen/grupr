package semantics

import (
	"fmt"
	"regexp"

	"github.com/rwberendsen/grupr/internal/syntax"
	"golang.org/x/exp/maps"
	"gopkg.in/yaml.v3"
)

type Grupin struct {
	UserGroups syntax.UserGroups
	ProducingServices map[string]syntax.ProducingService
	Products map[string]Product
	Interfaces map[syntax.InterfaceID]Interface
}

func NewGrupin(gSyn syntax.Grupin) (Grupin, error) {
	gSem := Grupin{gSyn.UserGroups, gSyn.ProducingServices}, map[string]Product{}, map[syntax.InterfaceID]Interface}
	for k, v := range gSyn.Products {
		if p, err := newProduct(v, gSem.UserGroups); err != nil {
			return gSem,  err
		} else {
			gSem.Products[k] = p
		}
	}
	for k, v := range gSyn.Interfaces {
	}
	if err := gSem.allConsumedOk(); err != nil {
		return gSem, err
	}
	if err := gSem.allDisjoint(); err != nil {
		return gSem, err
	}
	return gSem, nil
}

func (g Grupin) allConsumedOk() error {
	for _, p := range g.Products {
		for iid := range p.Consumes {
			if _, ok := g.Interfaces[iid]; !ok {
				return syntax.SetLogicError{fmt.Sprintf("product '%s': consumed interface '%s' not found", p.ID, iid)}
			}
			if p.Classification < g.Interfaces[iid].Classification {
				return PolicyError{fmt.Sprintf("product '%s' consumes interface with higher classification", p.ID)}
			}
		}
	}
	return nil
}

func (g Grups) allDisjoint() error {
	keys := maps.Keys(g.Products)
	if len(keys) < 2 {
		return nil
	}
	for i := 0; i < len(keys)-1; i++ {
		for j := i + 1; j < len(keys); j++ {
			if !g.Products[keys[i]].disjoint(g.Products[keys[j]]) {
				return syntax.SetLogicError{fmt.Sprintf("overlapping products '%s' and '%s'", keys[i], keys[j])}
			}
		}
	}
	return nil
}

func (g Grups) String() string {
	data, err := yaml.Marshal(g)
	if err != nil {
		panic("Grups could not be marshalled")
	}
	return string(data)
}
