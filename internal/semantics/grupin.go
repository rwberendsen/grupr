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
	Products map[string]Product
	Interfaces map[syntax.InterfaceID]InterfaceMetadata
	// TODO: ConsumingServices (e.g., a virtualisation tool, or, a file export tool)
	// TODO: Are we going to do anything with the BusinessPartner concept, where it could take the value of big customers, for example?
	// 	 And the ThirdParty concept, for when we ship data to a third party?
	// 	 And the Application concept (app), for when we ship data intended for a downstream application, a logical app, could be operational one, being outside of our system?
}

func NewGrupin(gSyn syntax.Grupin) (Grupin, error) {
	gSem := Grupin{
		AllowedUserGroups: gSyn.AllowedUserGroups,
		Products: map[string]Product{},
		Interfaces: map[syntax.InterfaceID]Interface,
	}
	for k, v := range gSyn.Products {
		if p, err := newProduct(v, gSem.AllowedUserGroups); err != nil {
			return gSem,  err
		} else {
			gSem.Products[k] = p
		}
	}
	for iid, v := range gSyn.Interfaces {
		if err := gSem.validateInterfaceID(iid); err != nil { return gSem, err }
		parent := &gSem.Products[iid.ProductID].InterfaceMetadata
		dtaps := gSem.Products[iid.ProductID].DTAPs.DTAPRendering
		if im, err := newInterfaceMetadata(v.InterfaceMetadata, gSem.AllowedUserGroups, dtaps, parent) {
			return fmt.Errorf("interface '%s': %w", iid, err)
		} else {
			gSem.Products[iid.ProductID][iid.ID] = im
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
			if _, ok := g.Products[iid.ProductID]; !ok {
				return SetLogicError{fmt.Sprintf("product '%s': consumed interface '%s': product not found", p.ID, iid)}
			}
			if _, ok := g.Products[iid.ProductID][iid.ID]; !ok {
				return SetLogicError{fmt.Sprintf("product '%s': consumed interface '%s': interface not found", p.ID, iid)}
			}
			// TODO: think: this policy also is already checked when creating the product semantic object, perhaps remove check in one of these places.
			if iid.ProductID == p.ID {
				return PolicyError{fmt.Sprintf("product '%s' not allowed to consume own interface '%s'", iid.ProductID, iid.ID)}
			}
			if p.Classification < g.Interfaces[iid].Classification {
				return PolicyError{fmt.Sprintf("product '%s' consumes interface with higher classification", p.ID)}
			}
		}
		for id, im := range p.Interfaces {
			if im.ForProduct != nil {
				if _, ok := g.Products[*im.ForProduct]; !ok {
					return SetLogicError{fmt.Sprintf("product '%s': interface '%s': product not found", p.ID, id)}
				}
				if *im.ForProduct == p.ID {
					return PolicyError{fmt.Sprintf("product '%s', interface '%s', ForProduct refers to self, but not allowed to consume own interface", p.ID, id)}
				}
			}
		}
	}
	return nil
}

func (g Grupin) allDisjoint() error {
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
	if _, ok := g.Products[iid.ProductID]; !ok {
		return SetLogicError{fmt.Sprintf("interface id '%s': product not found", iid)}
	}
	return nil
}

func (g Grupin) String() string {
	data, err := yaml.Marshal(g)
	if err != nil {
		panic("Grups could not be marshalled")
	}
	return string(data)
}
