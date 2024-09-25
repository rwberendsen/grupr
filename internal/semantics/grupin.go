package semantics

import (
	"fmt"
	"regexp"

	"github.com/rwberendsen/grupr/internal/syntax"
	"golang.org/x/exp/maps"
	"gopkg.in/yaml.v3"
)

type Grupin struct {
	ProducingServices map[string]syntax.ProducingService
	Products map[string]Product
	Interfaces map[InterfaceID]Interface
}

func NewGrupin(g_syn syntax.Grupin) (Grupin, error) {
	g_sem := Grupin{map[string]syntax.ProducingService{}, map[string]Product{}, map[InterfaceID]Interface}
	g_sem.ProducingServices = g_syn.ProducingServices
	for k, v := range g_syn.Products {
		if p, err := newProduct(v); err != nil {
			return g_sem, fmt.Errorf("product '%s': %s", k, err)
		} else {
			g_sem.Products[k] = p
		}
	}
	for k, v := range gSyn.Interfaces {
	}
	if err := g_sem.allConsumedOk(); err != nil {
		return g_sem, err
	}
	if err := g_sem.allDisjoint(); err != nil {
		return g_sem, err
	}
	return g_sem, nil
}

func (g Grups) allConsumedOk() error {
	for pid, p := range g.Products {
		for pi := range p.Consumes {
			if pi.Product == pid {
				return fmt.Errorf("consuming interface '%s' from own product '%s'", pi.Interface, pi.Product)
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
