package semantics

import (
	"fmt"
	"log"
	"time"

	"github.com/rwberendsen/grupr/internal/syntax"
)

type Grupin struct {
	Classes           map[string]syntax.Class
	GlobalUserGroups  GlobalUserGroups
	UserGroupMappings map[string]UserGroupMapping
	Products          map[string]Product
	// NB: ConsumingServices (e.g., a virtualisation tool, or, a file export tool) can be handled in a different top level YAML format
	// NB: Are we going to do anything with the BusinessPartner concept, where it could take the value of big customers, for example?
	// 	 And the ThirdParty concept, for when we ship data to a third party?
	// 	 And the Application concept (app), for when we ship data intended for a downstream application, a logical app, could be operational one, being outside of our system?
	//	 For now, these bigger changes are outside of the scope, and even when in scope, perhaps they can be handled outside of the Grupin data structure.
}

func NewGrupin(cnf *Config, gSyn syntax.Grupin) (Grupin, error) {
	start := time.Now()
	log.Printf("Validating deserialized YAML documents...\n")
	gSem := Grupin{
		Classes:           gSyn.Classes,
		GlobalUserGroups:  newGlobalUserGroups(*gSyn.GlobalUserGroups),
		UserGroupMappings: map[string]UserGroupMapping{},
		Products:          map[string]Product{},
	}
	// Validate user group mappings
	for k, v := range gSyn.UserGroupMappings {
		if ugm, err := newUserGroupMapping(v, gSem.GlobalUserGroups); err != nil {
			return gSem, err
		} else {
			gSem.UserGroupMappings[k] = ugm
		}
	}
	// Validate product specs
	for k, v := range gSyn.Products {
		if p, err := newProduct(cnf, v, gSem.Classes, gSem.GlobalUserGroups, gSem.UserGroupMappings); err != nil {
			return gSem, err
		} else {
			gSem.Products[k] = p
		}
	}
	// Validate interface specs
	for iid, v := range gSyn.Interfaces {
		if err := gSem.validateInterfaceID(iid); err != nil {
			return gSem, err
		}
		dtaps := gSem.Products[iid.ProductID].DTAPs.DTAPRendering
		parent := gSem.Products[iid.ProductID].InterfaceMetadata
		if im, err := newInterfaceMetadata(cnf, v.InterfaceMetadata, gSem.Classes, gSem.GlobalUserGroups, gSem.UserGroupMappings, dtaps, &parent); err != nil {
			return gSem, fmt.Errorf("interface '%s': %w", iid, err)
		} else {
			gSem.Products[iid.ProductID].Interfaces[iid.ID] = im
		}
	}
	// Validate DTAP and UserGroup tagging
	for k, v := range gSem.Products {
		if err := v.validateExprAttr(); err != nil {
			return gSem, fmt.Errorf("product '%s': %w", k, err)
		}
	}
	// Validate consume relationships
	if err := gSem.allConsumedOk(); err != nil {
		return gSem, err
	}
	// Validate all products are disjoint
	if err := gSem.allDisjoint(); err != nil {
		return gSem, err
	}
	t := time.Now()
	log.Printf("Validating deserialized YAML documents took %v\n", t.Sub(start))
	return gSem, nil
}

func (g Grupin) allConsumedOk() error {
	for _, p := range g.Products {
		for iid, dtapMapping := range p.Consumes {
			if _, ok := g.Products[iid.ProductID]; !ok {
				return &SetLogicError{fmt.Sprintf("product '%s': consumed interface '%s': product not found", p.ID, iid)}
			}
			if _, ok := g.Products[iid.ProductID].Interfaces[iid.ID]; !ok {
				return &SetLogicError{
					fmt.Sprintf("product '%s': consumed interface '%s': interface not found", p.ID, iid),
				}
			}
			// TODO: think: this policy is also checked when creating the product semantic object, perhaps remove check in one of these places.
			if iid.ProductID == p.ID {
				return &PolicyError{fmt.Sprintf("product '%s' not allowed to consume own interface '%s'", iid.ProductID, iid.ID)}
			}
			if p.Classification < g.Products[iid.ProductID].Interfaces[iid.ID].Classification {
				return &PolicyError{fmt.Sprintf("product '%s' consumes interface with higher classification", p.ID)}
			}
			// Check specified DTAP mappings
			for _, dtapSource := range dtapMapping {
				if !g.Products[iid.ProductID].DTAPs.HasDTAP(dtapSource)
					return &SetLogicError{fmt.Sprintf("product '%s': consumed interface '%s': dtap '%s': dtap not found", p.ID, iid, dtapSource)}
				}
			}
			// For non production DTAPs not specified in dtapMapping, check that a non-prod DTAP with the same name exists in the consumed product;
			// It should not be a prod DTAP in the consumed product; because in a one to one mapping we would expect not just the names but also
			// the nature of the DTAPs to be the same; 
			for dtapSelf, _ := range p.DTAPs.NonProd {
				if _, ok := dtapMapping[dtapSelf]; !ok {
					if _, ok := g.Products[iid.ProductID].DTAPs.NonProd[dtapSelf]; !ok {
						return &SetLogicError{fmt.Sprintf("product '%s': consumed interface '%s': dtap '%s': dtap not found", p.ID, iid, dtapSelf)}
					}
				}
			}
		}
		for id, im := range p.Interfaces {
			if im.ForProduct != nil {
				if _, ok := g.Products[*im.ForProduct]; !ok {
					return &SetLogicError{fmt.Sprintf("product '%s': interface '%s': product not found", p.ID, id)}
				}
				if *im.ForProduct == p.ID {
					return &PolicyError{fmt.Sprintf("product '%s', interface '%s', ForProduct refers to self, but not allowed to consume own interface", p.ID, id)}
				}
			}
		}
	}
	return nil
}

func (g Grupin) allDisjoint() error {
	if len(g.Products) < 2 {
		return nil
	}
	var keys []string
	for k := range g.Products {
		keys = append(keys, k)
	}
	for i := 0; i < len(keys)-1; i++ {
		for j := i + 1; j < len(keys); j++ {
			if !g.Products[keys[i]].disjoint(g.Products[keys[j]]) {
				return &SetLogicError{fmt.Sprintf("overlapping products '%s' and '%s'", keys[i], keys[j])}
			}
		}
	}
	return nil
}

func (g Grupin) validateInterfaceID(iid syntax.InterfaceID) error {
	if _, ok := g.Products[iid.ProductID]; !ok {
		return &SetLogicError{fmt.Sprintf("interface id '%s': product not found", iid)}
	}
	return nil
}
