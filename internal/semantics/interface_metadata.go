package semantics

import (
	"fmt"
	"maps"

	"github.com/rwberendsen/grupr/internal/syntax"
)

type InterfaceMetadata struct {
	ObjectMatchers   ObjMatchers
	Classification   Classification
	GlobalUserGroups map[string]bool
	UserGroups       syntax.Rendering
	MaskColumns      ColMatcher
	HashColumns      ColMatcher
	ConsumedBy       map[string]map[ProductDTAPID]struct{} // will be populated by Grupin.allConsumedOK
	ExposeDTAPs      map[string]bool                       // TODO: convert this to HideDTAPs, to be unioned between product and interface
	ForProduct       *string
}

func newInterfaceMetadata(cnf *Config, imSyn syntax.InterfaceMetadata, classes map[string]syntax.Class, globalUserGroups map[string]bool,
	userGroupMapping UserGroupMapping, dtaps syntax.Rendering, parent *InterfaceMetadata) (InterfaceMetadata, error) {
	imSem := InterfaceMetadata{}
	if err := imSem.setClassification(imSyn, parent, classes); err != nil {
		return imSem, err
	}
	if err := imSem.setUserGroups(imSyn, parent, globalUserGroups, userGroupMapping); err != nil {
		return imSem, err
	}
	// TODO: replace with HideDTAPs
	if err := imSem.setExposeDTAPs(imSyn, parent, dtaps); err != nil {
		return imSem, err
	}
	if err := imSem.setObjectMatchers(cnf, imSyn, parent, dtaps); err != nil {
		return imSem, err
	}
	if err := imSem.setMaskColumns(cnf, imSyn, parent, dtaps); err != nil {
		return imSem, err
	}
	if err := imSem.setHashColumns(cnf, imSyn, parent, dtaps); err != nil {
		return imSem, err
	}
	if err := imSem.setForProduct(imSyn, parent, dtaps); err != nil {
		return imSem, err
	}
	if parent != nil {
		imSem.ConsumedBy = map[string]map[ProductDTAPID]struct{}{}
		// TODO: take into account hidden DTAPs
		for d := range dtaps {
			imSem.ConsumedBy[d] = map[ProductDTAPID]struct{}{} // will be further populated by Grupin.allConsumedOK
		}
	}
	return imSem, nil
}

func (imSem *InterfaceMetadata) setClassification(imSyn syntax.InterfaceMetadata, parent *InterfaceMetadata, classes map[string]syntax.Class) error {
	if imSyn.Classification == "" {
		if parent != nil {
			imSem.Classification = parent.Classification
			return nil
		}
		return &PolicyError{"Classfication is a required field on product level"}
	}
	if c, err := newClassification(imSyn.Classification, classes); err != nil {
		return err
	} else {
		imSem.Classification = c
	}
	if parent != nil && parent.Classification < imSem.Classification {
		return &PolicyError{"Classification on interface higher than product classification"}
	}
	return nil
}

func getGlobalUserGroup(userGroup string, userGroupMapping UserGroupMapping, globalUserGroups map[string]bool) (string, bool) {
	if userGroupMapping == nil {
		// product did not define a user group mapping id
		_, ok := globalUserGroups[userGroup]
		return userGroup, ok // userGroup is a global user group
	}
	globalUserGroup, ok := userGroupMapping[userGroup]
	return globalUserGroup, ok
}

func (imSem *InterfaceMetadata) setUserGroups(imSyn syntax.InterfaceMetadata, parent *InterfaceMetadata,
	globalUserGroups map[string]bool, userGroupMapping UserGroupMapping) error {
	if imSyn.UserGroups == nil {
		if parent != nil {
			imSem.UserGroups = parent.UserGroups
			imSem.GlobalUserGroups = parent.GlobalUserGroups
		}
		return nil
	}
	imSem.UserGroups = syntax.Rendering{}
	imSem.GlobalUserGroups = map[string]bool{}
	for _, u := range imSyn.UserGroups {
		if g, ok := getGlobalUserGroup(u, userGroupMapping, globalUserGroups); !ok {
			return &SetLogicError{fmt.Sprintf("Unknown user group: %s", u)}
		} else {
			imSem.GlobalUserGroups[g] = true
		}
		imSem.UserGroups[u] = u
	}
	for u, r := range imSyn.UserGroupRendering {
		imSem.UserGroups[u] = r
	}
	if parent != nil {
		// interfaces should have a subset of global user groups with regard to parent product
		for u := range imSem.GlobalUserGroups {
			if _, ok := parent.GlobalUserGroups[u]; !ok {
				return &PolicyError{fmt.Sprintf("Interface should not have global user group '%s' that product does not have", u)}
			}
		}
		// if parent product has user groups, interface should also have a at least one
		if len(parent.GlobalUserGroups) > 0 && len(imSem.GlobalUserGroups) == 0 {
			return &PolicyError{fmt.Sprintf("Product has global user groups, so interface should have at least one also", u)}
		}
	}
	return nil
}

func (imSem *InterfaceMetadata) setExposeDTAPs(imSyn syntax.InterfaceMetadata, parent *InterfaceMetadata,
	dtaps syntax.Rendering) error {
	if imSyn.ExposeDTAPs == nil {
		if parent != nil {
			imSem.ExposeDTAPs = parent.ExposeDTAPs
		}
		return nil
	}
	imSem.ExposeDTAPs = make(map[string]bool, len(dtaps))
	for _, d := range imSyn.ExposeDTAPs {
		if _, ok := imSem.ExposeDTAPs[d]; ok {
			return &syntax.FormattingError{fmt.Sprintf("ExposeDTAPs: duplicate dtap '%s'", d)}
		}
		if _, ok := dtaps[d]; !ok {
			return &SetLogicError{fmt.Sprintf("ExposeDTAPs: unknown dtap '%s'", d)}
		}
		imSem.ExposeDTAPs[d] = true
	}
	return nil
}

func (imSem *InterfaceMetadata) setObjectMatchers(cnf *Config, imSyn syntax.InterfaceMetadata, parent *InterfaceMetadata,
	dtaps syntax.Rendering) error {
	if imSyn.Objects == nil {
		if parent != nil {
			imSem.ObjectMatcher = parent.ObjectMatcher
			return nil
		}
		return &PolicyError{"ObjectMatcher is a required field"}
	}
	if m, err := newObjMatchers(cnf, imSyn.Objects, imSyn.ObjectsExclude, dtaps, imSem.UserGroups); err != nil {
		return fmt.Errorf("ObjectMatchers: %w", err)
	} else {
		imSem.ObjectMatchers = m
	}
	if parent != nil {
		if !imSem.ObjectMatchers.subsetOf(parent.ObjectMatchers) {
			return &PolicyError{"ObjectMatcher should be a subset of parent ObjectMatcher"}
		}
		imSem.ObjectMatchers = imSem.ObjectMatchers.setSubsetOf(parent.ObjectMatchers)
	}
	return nil
}

func (imSem *InterfaceMetadata) setHashColumns(cnf *Config, imSyn syntax.InterfaceMetadata, parent *InterfaceMetadata, dtaps syntax.Rendering) error {
	if imSyn.MaskColumns == nil {
		if parent != nil {
			imSem.HashColumns = parent.HashColumns
		}
		return nil
	}
	if m, err := newColMatcher(cnf, imSyn.HashColumns, dtaps, imSem.UserGroups, imSem.ObjectMatcher); err != nil {
		return fmt.Errorf("hash_columns: %w", err)
	} else {
		imSem.HashColumns = m
	}
	return nil
}

func (imSem *InterfaceMetadata) setMaskColumns(cnf *Config, imSyn syntax.InterfaceMetadata, parent *InterfaceMetadata, dtaps syntax.Rendering) error {
	if imSyn.MaskColumns == nil {
		if parent != nil {
			imSem.MaskColumns = parent.MaskColumns
		}
		return nil
	}
	if m, err := newColMatcher(cnf, imSyn.MaskColumns, dtaps, imSem.UserGroups, imSem.ObjectMatcher); err != nil {
		return fmt.Errorf("mask_columns: %w", err)
	} else {
		imSem.MaskColumns = m
	}
	return nil
}

func (imSem *InterfaceMetadata) setForProduct(imSyn syntax.InterfaceMetadata, parent *InterfaceMetadata, dtaps syntax.Rendering) error {
	if imSyn.ForProduct == nil {
		if parent != nil {
			imSem.ForProduct = parent.ForProduct
		}
		return nil
	}
	imSem.ForProduct = imSyn.ForProduct
	return nil
}

func equal_pointer_string(lhs *string, rhs *string) bool {
	// TODO: check if a simple generic exists for the three lines below, and if so, use it.
	if lhs != rhs {
		if lhs == nil || rhs == nil {
			return false
		}
		if *lhs != *rhs {
			return false
		}
	}
	return true
}

func (lhs InterfaceMetadata) Equal(rhs InterfaceMetadata) bool {
	return lhs.ObjectMatchers.Equal(rhs.ObjectMatchers) &&
		lhs.Classification == rhs.Classification &&
		maps.Equal(lhs.GlobalUserGroups, rhs.GlobalUserGroups) &&
		lhs.UserGroups.Equal(rhs.UserGroups) &&
		lhs.MaskColumns.Equal(rhs.MaskColumns) &&
		lhs.HashColumns.Equal(rhs.MaskColumns) &&
		maps.Equal(lhs.ExposeDTAPs, rhs.ExposeDTAPs) &&
		equal_pointer_string(lhs.ForProduct, rhs.ForProduct)
}
