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
}

func NewGrupin(gSyn syntax.Grupin) (Grupin, error) {
	gSem := Grupin{gSyn.UserGroups, gSyn.ProducingServices}, map[string]Product{}}
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

func (g Grups) allConsumedOk() error {
	for pid, p := range g.Products {
		for pi := range p.Consumes {
			if pi.Product == pid {
				return PolicyError{fmt.Sprintf("consuming interface '%s' from own product '%s'", pi.Interface, pi.Product)}
			}
			if pUpstream, ok := g.Products[pi.Product]; !ok {
				return fmt.Errorf("product '%s': consumed product '%s' not found", pid, pi.Product)
			} else if _, ok := pUpstream.Interfaces[pi.Interface]; !ok {
				return fmt.Errorf("product '%s': consumed interface '%s' from product '%s' not found", pid, pi.Interface, pi.Product)
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
				return fmt.Errorf("overlapping products '%s' and '%s'", keys[i], keys[j])
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
