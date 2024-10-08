package semantics

import (
	"fmt"
	"regexp"

	"github.com/rwberendsen/grupr/internal/syntax"
	"golang.org/x/exp/maps"
	"gopkg.in/yaml.v3"
)

type Grupin struct {
	// TODO: Classification: enable user to supply classifications with short name, long name, and integer value
	AllowedUserGroups map[string]bool
	ProducingServices map[string]syntax.ProducingService
	Products map[string]Product
	Interfaces map[syntax.InterfaceID]InterfaceMetadata
	// TODO: ConsumingServices (e.g., a virtualisation tool, or, a file export tool)
}

func NewGrupin(gSyn syntax.Grupin) (Grupin, error) {
	gSem := Grupin{
		AllowedUserGroups: gSyn.AllowedUserGroups,
		ProducinServices: map[string]ProducingService{},
		Products: map[string]Product{},
		Interfaces: map[syntax.InterfaceID]Interface,
	}
	for k, v := range gSyn.ProducingServices {
		gSem[k] = newProducingService(v)
	}
	for k, v := range gSyn.Products {
		if p, err := newProduct(v, gSem.AllowedUserGroups); err != nil {
			return gSem,  err
		} else {
			gSem.Products[k] = p
		}
	}
	for iid, v := range gSyn.Interfaces {
		if err := gSem.validateInterfaceID(iid); err != nil { return err }
		var parent *InterfaceMetadata
		var dtaps syntax.Rendering
		if iid.IsProductInterface() {
			parent = &gSem.Products[iid.ProductID].InterfaceMetadata
			dtaps = gSem.Products[iid.ProductID].DTAPs.DTAPRendering
		} else { // iid.IsProducingServiceInterface() == true
			dtaps = gSem.ProducingServices[iid.ProducingServiceID].DTAPs.DTAPRendering
		}
		if im, err := newInterfaceMetadata(v.InterfaceMetadata, gSem.AllowedUserGroups, dtaps, parent) {
			return fmt.Errorf("interface '%s': %w", iid, err)
		} else {
			gSem.Interfaces[iid] = im
		}
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
				return SetLogicError{fmt.Sprintf("product '%s': consumed interface '%s' not found", p.ID, iid)}
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
				return SetLogicError{fmt.Sprintf("overlapping products '%s' and '%s'", keys[i], keys[j])}
			}
		}
	}
	return nil
}

func (g Grupin) validateInterfaceID(iid syntax.InterfaceID) error {
	if iid.ProducingServiceID != "" {
		if _, ok := g.ProducingServices[iid.ProducingServiceID]; !ok {
			return SetLogicError{fmt.Sprintf("interface id '%s': producing service not found", iid)}
		}
	}
	if _, ok := g.Products[iid.ProductID]; !ok {
		return SetLogicError{fmt.Sprintf("interface id '%s': product not found", iid)}
	}
}

func (g Grups) String() string {
	data, err := yaml.Marshal(g)
	if err != nil {
		panic("Grups could not be marshalled")
	}
	return string(data)
}
