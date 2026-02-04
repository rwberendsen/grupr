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
		if im, err := newInterfaceMetadata(cnf, v.InterfaceMetadata, gSem.Classes, gSem.GlobalUserGroups, gSem.UserGroupMappings[parent.UserGroupMapping], dtaps, &parent); err != nil {
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
			pSource, ok := g.Products[iid.ProductID]
			if !ok {
				return &SetLogicError{fmt.Sprintf("product '%s': consumed interface '%s': product not found", p.ID, iid)}
			}
			iSource, ok := pSource.Interfaces[iid.ID]
			if !ok {
				return &SetLogicError{
					fmt.Sprintf("product '%s': consumed interface '%s': interface not found", p.ID, iid),
				}
			}
			// TODO: think: this policy is also checked when creating the product semantic object, perhaps remove check in one of these places.
			if iid.ProductID == p.ID {
				return &PolicyError{fmt.Sprintf("product '%s' not allowed to consume own interface '%s'", iid.ProductID, iid.ID)}
			}
			if p.Classification < iSource.Classification {
				// TODO: consider removing this policy rule, possibly too strict
				// It might be useful to keep it, if e.g., you are using masking or hashing directives in the YAML,
				// then when you define those, you lower the classification of the interface accordingly;
				// you can have a separate interface for the same tables where you do not use such directives, and
				// where you keep the higher classification.
				// and then you implement some mechanism by which you make sure if a consumer consumes the
				// interface with masking or hashing directives, that the consumer indeed does not consume the
				// unmasked and unhashed data.
				return &PolicyError{fmt.Sprintf("product '%s' consumes interface with higher classification", p.ID)}
			}

			// Check DTAP mapping
			// TODO: add hide_dtaps to interface metadata, and union product level and interface level, and check here that hidden dtaps are not consumed.
			for dtapSelf, dtapSource := range dtapMapping {
				if !pSource.DTAPs.HasDTAP(dtapSource) {
					return &SetLogicError{fmt.Sprintf("product '%s': consumed interface '%s': dtap '%s': dtap not found", p.ID, iid, dtapSource)}
				}
				if p.DTAPs.IsProd(dtapSelf) && !pSource.DTAPs.IsProd(dtapSource) {
					return &PolicyError{fmt.Sprintf("product '%s': consumed interface '%s': prod dtap not allowed to consume interface from non-prod dtap", p.ID, iid)}
				}
				// Even though iSource is a copy, all copies reference the same map, initialized upon creation by NewInterface
				// So we can reach into that map here and add an element to it
				iSource.ConsumedBy[dtapSource][ProductDTAPID{ProductID: p.ID, DTAP: dtapSelf}] = struct{}{}
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
