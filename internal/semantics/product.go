package semantics

import (
	"fmt"
	"maps"

	"github.com/rwberendsen/grupr/internal/syntax"
)

type Product struct {
	ID       string 
	DTAPs    DTAPSpec
	UserGroupMapping string
	InterfaceMetadata
	UserGroupColumn  ColMatcher
	Consumes map[syntax.InterfaceID]map[string]string
	Interfaces map[string]InterfaceMetadata
}

func newProduct(cnf *Config, pSyn syntax.Product, classes map[string]syntax.Class, globalUserGroups map[string]bool,
	userGroupMappings map[string]UserGroupMapping) (Product, error) {
	// Initialize
	pSem := Product{
		ID:         pSyn.ID,
		DTAPs:      newDTAPSpec(pSyn.DTAPs, pSyn.DTAPRendering),
		Interfaces: map[string]InterfaceMetadata{},
	}
	// Set UsergroupMapping
	if _, ok := userGroupMappings[pSyn.UserGroupMapping]; !ok {
		return &SetLogicError{fmt.Sprintf("Unknown user group mapping: '%s'", pSyn.UserGroupMapping)}
	}
	pSem.UserGroupMapping = pSyn.UserGroupMapping
	// Set InterfaceMetadata
	if im, err := newInterfaceMetadata(cnf, pSyn.InterfaceMetadata, classes, globalUserGroups, userGroupMappings, pSem.DTAPs.DTAPRendering, nil); err != nil {
		return pSem, fmt.Errorf("product id %s: interface metadata: %w", pSem.ID, err)
	} else {
		pSem.InterfaceMetadata = im
	}
	// Set UserGroupColumn (this requires InterfaceMetadata its ObjectMatchers to be set)
	if err := pSem.setUserGroupColumn(pSyn); err != nil {
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
		for k, _ := range cs.DTAPMapping {
			if !pSem.DTAPs.HasDTAP(k) {
				return pSem, fmt.Errorf("Unknown DTAP specified in consumption spec dtap mapping")
			}
		}
		pSem.Consumes[cs.InterfaceID] = cs.DTAPMapping
	}
	return pSem, nil
}

func (pSem *Product) setUserGroupColumn(pSyn syntax.Product, dtaps syntax.Rendering) error {
	if pSyn.UserGroupColumn == "" {
		return nil
	}
	if m, err := newColMatcher([]string{pSyn.UserGroupColumn}, pSem.DTAPs.DTAPRendering, pSem.UserGroups, pSem.ObjectMatchers); err != nil {
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
}

func (lhs Product) disjoint(rhs Product) bool {
	return lhs.ObjectMatchers.disjoint(rhs.ObjectMatchers)
}

func (lhs Product) Equal(rhs Product) bool {
	if lhs.ID != rhs.ID { return false }
	if !lhs.DTAPs.Equal(rhs.DTAPs) { return false }
	if lhs.UserGroupMapping != rhs.UserGroupMapping { return false }
	if !lhs.InterfaceMetadata.Equal(rhs.InterfaceMetadata) { return false }
	if !lhs.UserGroupColumn.Equal(rhs.UserGroupColumn) { return false }
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
	if !maps.EqualFunc(lhs.Interfaces, rhs.Interfaces, InterfaceMetadata.Equal) {
		return false
	}
	return true
}
