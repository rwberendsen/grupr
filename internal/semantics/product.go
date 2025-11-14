package semantics

import (
	"fmt"
	"maps"

	"github.com/rwberendsen/grupr/internal/syntax"
)

type Product struct {
	ID       string                      		  `yaml:"id"`
	DTAPs    DTAPSpec                    		  `yaml:"dtaps,flow,omitempty"`
	Consumes map[syntax.InterfaceID]map[string]string `yaml:",omitempty"`
	InterfaceMetadata
	Interfaces map[string]InterfaceMetadata
}

func newProduct(cnf *Config, pSyn syntax.Product, classes map[string]syntax.Class, globalUserGroups map[string]bool,
	userGroupMappings map[string]UserGroupMapping) (Product, error) {
	pSem := Product{
		ID:         pSyn.ID,
		DTAPs:      newDTAPSpec(pSyn.DTAPs, pSyn.DTAPRendering),
		Interfaces: map[string]InterfaceMetadata{},
	}
	if im, err := newInterfaceMetadata(cnf, pSyn.InterfaceMetadata, classes, globalUserGroups, userGroupMappings, pSem.DTAPs.DTAPRendering, nil); err != nil {
		return pSem, fmt.Errorf("product id %s: interface metadata: %w", pSem.ID, err)
	} else {
		pSem.InterfaceMetadata = im
	}
	pSem.Consumes = map[syntax.InterfaceID]map[string]string{}
	for _, cs := range pSyn.Consumes {
		if _, ok := pSem.Consumes[cs.InterfaceID]; ok {
			return pSem, fmt.Errorf("duplicate consumed interface id")
		}
		if cs.ProductID == pSem.ID {
			return pSem, &PolicyError{
				fmt.Sprintf("product '%s' not allowed to consume own interface '%s'", cs.ProductID, cs.ID),
			}
		}
		for k, _ := range cs.DTAPMapping {
			if _, ok := pSem.DTAPs.NonProd[k]; !ok {
				return pSem, fmt.Errorf("Unknown non-production DTAP specified in consumption spec dtap mapping")
			}
		}
		if cs.DTAPMapping == nil {
			pSem.Consumes[cs.InterfaceID] = map[string]string{} 
		} else {
			pSem.Consumes[cs.InterfaceID] = cs.DTAPMapping
		}
	}
	return pSem, nil
}

func (lhs Product) disjoint(rhs Product) bool {
	return lhs.ObjectMatchers.disjoint(rhs.ObjectMatchers)
}

func (lhs Product) Equal(rhs Product) bool {
	if lhs.ID != rhs.ID {
		return false
	}
	if !lhs.DTAPs.Equal(rhs.DTAPs) {
		return false
	}
	for lhsKey, lhsValue := range lhs.Consumes {
		if rhsValue, ok := rhs.Consumes[lhsKey]; !ok {
			return false
		} else {
			if !maps.Equal(lhsValue, rhsValue) { return false }
		}
	}
	for rhsKey, _ := range rhs.Consumes {
		if _, ok := lhs.Consumes[rhsKey]; !ok { return false }
	}
	if !lhs.InterfaceMetadata.Equal(rhs.InterfaceMetadata) {
		return false
	}
	if !maps.EqualFunc(lhs.Interfaces, rhs.Interfaces, InterfaceMetadata.Equal) {
		return false
	}
	return true
}
