package semantics

import (
	"fmt"
	"os"

	"github.com/rwberendsen/grupr/internal/syntax"
)

type Grupin struct {
	Classes           map[string]syntax.Class
	GlobalUserGroups  GlobalUserGroups
	UserGroupMappings map[string]UserGroupMapping
	Products          map[string]Product
	ServiceAccounts   map[string]ServiceAccount
}

func NewGrupin(cnf *Config, gSyn syntax.Grupin) (Grupin, error) {
	gSem := Grupin{
		Classes:           gSyn.Classes,
		UserGroupMappings: map[string]UserGroupMapping{},
		Products:          map[string]Product{},
		ServiceAccounts:   map[string]ServiceAccount{},
	}
	// Validate class labels; they should be valid ids
	for k, _ := range gSem.Classes {
		if _, err := NewID(cnf, k); err != nil {
			return gSem, fmt.Errorf("classes: %w", err)
		}
	}

	// Validate global user groups, they should be valid ids
	if gug, err := newGlobalUserGroups(cnf, gSyn.GlobalUserGroups); err != nil {
		return gSem, err
	} else {
		gSem.GlobalUserGroups = gug
	}

	// Validate user group mappings
	// Add identity mapping for ease of reference
	gSem.UserGroupMappings[""] = UserGroupMapping{}
	for k, _ := range gSem.GlobalUserGroups {
		gSem.UserGroupMappings[""][k] = k
	}
	for k, v := range gSyn.UserGroupMappings {
		if _, ok := gSem.UserGroupMappings[k]; ok {
			return gSem, fmt.Errorf("duplicate user group mapping")
		}
		if ugm, err := newUserGroupMapping(cnf, v, gSem.GlobalUserGroups); err != nil {
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
		if _, err := NewID(cnf, iid.ID); err != nil {
			return gSem, &SetLogicError{fmt.Sprintf("interface id '%s' its ID field: %w", iid, err)}
		}
		if parentProduct, ok := gSem.Products[iid.ProductID]; !ok {
			return gSem, &SetLogicError{fmt.Sprintf("interface id '%s': product not found", iid)}
		} else {
			ds := parentProduct.DTAPs
			userGroupMapping := gSem.UserGroupMappings[parentProduct.UserGroupMappingID]
			userGroupRenderings := parentProduct.UserGroupRenderings
			parent := parentProduct.InterfaceMetadata
			if im, err := newInterfaceMetadata(cnf, v.InterfaceMetadata, gSem.Classes, ds, userGroupMapping, userGroupRenderings, &parent); err != nil {
				return gSem, fmt.Errorf("interface '%s': %w", iid, err)
			} else {
				parentProduct.Interfaces[iid.ID] = im
			}
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
	// Validate service accounts
	// TODO WIP validate that rendered ident exprs are unique accross service accounts
	for k, v := range gSyn.ServiceAccounts {
		if svc, err := newServiceAccount(cnf, v, gSem.Products); err != nil {
			return gSem, err
		} else {
			gSem.ServiceAccounts[k] = svc
		}
	}
	return gSem, nil
}

func NewGrupinFromPath(cnf *Config, path string) (Grupin, error) {
	var g Grupin
	f, err := os.Open(path)
	defer f.Close()
	if err != nil {
		return g, err
	}
	s, err := syntax.NewGrupin(f) // redeclaring err, which just gets assigned a new value
	if err != nil {
		return g, err
	}
	return NewGrupin(cnf, s)
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
			// TODO: add hide_dtaps to interface metadata, union product level and interface level, and check here that hidden dtaps are not consumed.
			for dtapSelf, dtapSource := range dtapMapping {
				if p.DTAPs.IsProd(dtapSelf) {
					if !pSource.DTAPs.HasProd() {
						// TODO: when source has hidden dtaps, consider that here, too
						return &PolicyError{fmt.Sprintf("product '%s': consumed interface '%s': source has no prod dtap", p.ID, iid)}
					}
					dtapSource = *pSource.DTAPs.Prod
					dtapMapping[dtapSelf] = dtapSource
				} else if !pSource.DTAPs.HasDTAP(dtapSource) {
					return &SetLogicError{fmt.Sprintf("product '%s': consumed interface '%s': dtap '%s': dtap not found", p.ID, iid, dtapSource)}
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
