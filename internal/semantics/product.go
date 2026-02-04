package semantics

import (
	"fmt"
	"maps"

	"github.com/rwberendsen/grupr/internal/syntax"
)

type Product struct {
	ID                 string
	DTAPs              DTAPSpec
	UserGroupMappingID string
	UserGroupRendering syntax.Rendering
	InterfaceMetadata
	UserGroupColumn    ColMatcher
	Consumes   map[syntax.InterfaceID]map[string]string
	Interfaces map[string]InterfaceMetadata
}

func newProduct(cnf *Config, pSyn syntax.Product, classes map[string]syntax.Class, globalUserGroups map[string]bool,
	userGroupMappings map[string]UserGroupMapping) (Product, error) {
	// Initialize
	pSem := Product{
		ID:         pSyn.ID,
		DTAPs:      newDTAPSpec(cnf, pSyn.DTAPs, pSyn.DTAPRendering),
		Interfaces: map[string]InterfaceMetadata{},
	}

	// Set UsergroupMappingID
	if pSyn.UserGroupMappingID != "" {
		if _, ok := userGroupMappings[pSyn.UserGroupMappingID]; !ok {
			return pSem, &SetLogicError{fmt.Sprintf("unknown user group mapping id: '%s'", pSyn.UserGroupMappingID)}
		}
	}
	pSem.UserGroupMappingID = pSyn.UserGroupMappingID

	// Set UserGroupRendering
	for k := range pSyn.UserGroupRendering {
		if _, ok := userGroupMappings[pSem.UserGroupMappingID][k]; !ok {
			return pSem, &SetLogicError{fmt.Sprintf("unknown user group in rendering: '%s'", k)}
		}
	}
	pSem.UserGroupRendering = pSyn.UserGroupRendering

	// Set InterfaceMetadata
	if im, err := newInterfaceMetadata(cnf, pSyn.InterfaceMetadata, classes, pSem.DTAPs.DTAPRendering, userGroupMappings[pSem.UserGroupMappingID], pSem.UserGroupRendering, nil); err != nil {
		return pSem, fmt.Errorf("product id %s: interface metadata: %w", pSem.ID, err)
	} else {
		pSem.InterfaceMetadata = im
	}

	// Set UserGroupColumn (this requires InterfaceMetadata its ObjectMatchers to be set)
	if err := pSem.setUserGroupColumn(cnf, pSyn); err != nil {
		return pSem, err
	}

	// Set Consumes
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
		// If a dtap mapping is specified, it means
		// - product only wants to consume source interface in specified dtaps
		// - designated source dtaps have to exist (though they are allowed to be hidden)
		// If no dtap mapping is specified, it is interpreted as if all DTAPs want to consume from a source DTAP with the same name.
		if cs.DTAPMapping != nil {
			for k, _ := range cs.DTAPMapping {
				if !pSem.DTAPs.HasDTAP(k) {
					return pSem, fmt.Errorf("Unknown DTAP specified in consumption spec dtap mapping")
				}
			}
			pSem.Consumes[cs.InterfaceID] = cs.DTAPMapping
		} else {
			// Default DTAP Mapping is expecting a dtap with the same name in consumed product
			pSem.Consumes[cs.InterfaceID] = map[string]string{}
			for dtap, _ := range pSem.DTAPs.All() {
				pSem.Consumes[cs.InterfaceID][dtap] = dtap
			}
		}
	}
	return pSem, nil
}

func (pSem *Product) setUserGroupColumn(cnf *Config, pSyn syntax.Product) error {
	if pSyn.UserGroupColumn == "" {
		return nil
	}
	if m, err := newColMatcher(cnf, []string{pSyn.UserGroupColumn}, pSem.DTAPs.DTAPRendering, pSem.UserGroups, pSem.ObjectMatchers); err != nil {
		return fmt.Errorf("user_group_column: %w", err)
	} else {
		pSem.UserGroupColumn = m
	}
	return nil
}

func (pSem *Product) validateExprAttr() error {
	for id, im := range pSem.Interfaces {
		// we know that ObjMatchers of interfaces are subsets of pSem.ObjectMatchers
		if err := im.ObjectMatchers.validateExprAttrAgainst(pSem.ObjectMatchers); err != nil {
			return fmt.Errorf("interface '%s': %w", id, err)
		}
	}
	return nil
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
	if lhs.UserGroupMappingID != rhs.UserGroupMappingID {
		return false
	}
	if !lhs.UserGroupRendering.Equal(rhs.UserGroupRendering) {
		return false
	}
	if !lhs.InterfaceMetadata.Equal(rhs.InterfaceMetadata) {
		return false
	}
	if !lhs.UserGroupColumn.Equal(rhs.UserGroupColumn) {
		return false
	}
	for lhsKey, lhsValue := range lhs.Consumes {
		if rhsValue, ok := rhs.Consumes[lhsKey]; !ok {
			return false
		} else {
			if !maps.Equal(lhsValue, rhsValue) {
				return false
			}
		}
	}
	for rhsKey, _ := range rhs.Consumes {
		if _, ok := lhs.Consumes[rhsKey]; !ok {
			return false
		}
	}
	if !maps.EqualFunc(lhs.Interfaces, rhs.Interfaces, InterfaceMetadata.Equal) {
		return false
	}
	return true
}
