package semantics

import (
	"fmt"
	"maps"

	"github.com/rwberendsen/grupr/internal/syntax"
	"github.com/rwberendsen/grupr/internal/util"
)

type InterfaceMetadata struct {
	ObjectMatchers           ObjMatchers
	Classification           Classification
	UserGroups               syntax.Rendering
	MaskColumns              ColMatcher
	HashColumns              ColMatcher
	ConsumedBy               map[string]map[ProductDTAPID]struct{} // will be populated by Grupin.allConsumedOK
	ForProduct               *string
}

func newInterfaceMetadata(cnf *Config, imSyn syntax.InterfaceMetadata, classes map[string]syntax.Class, ds DTAPSpec, userGroupMapping UserGroupMapping,
	userGroupRenderings map[string]syntax.Rendering, parent *InterfaceMetadata) (InterfaceMetadata, error) {
	imSem := InterfaceMetadata{}
	if err := imSem.setClassification(imSyn, parent, classes); err != nil {
		return imSem, err
	}
	if err := imSem.setUserGroups(imSyn, parent, userGroupMapping, userGroupRenderings); err != nil {
		return imSem, err
	}
	if err := imSem.setObjectMatchers(cnf, imSyn, parent, ds); err != nil {
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

func (imSem *InterfaceMetadata) setUserGroups(imSyn syntax.InterfaceMetadata, parent *InterfaceMetadata, userGroupMapping UserGroupMapping,
	userGroupRendering syntax.Rendering) error {
	if parent == nil {
		// this is a product-level interface
		if imSyn.UserGroups == nil {
			return nil
		}
		imSem.UserGroups = syntax.Rendering{}
		for _, u := range imSyn.UserGroups {
			if _, ok := userGroupMapping[u]; !ok {
				return &SetLogicError{fmt.Sprintf("unknown user group: '%s'", u)}
			}
			// this is a valid user group
			if rendering, ok := userGroupRendering[u]; ok {
				imSem.UserGroups[u] = rendering
			} else {
				imSem.UserGroups[u] = u
			}
		}
		return nil
	}

	// we have a parent (product-level) interface
	if len(imSyn.UserGroups) == 0 {
		imSem.UserGroups = parent.UserGroups
		return nil
	}
	imSem.UserGroups = syntax.Rendering{}
	for _, u := range imSyn.UserGroups {
		if rendering, ok := parent.UserGroups[u]; !ok {
			return &PolicyError{fmt.Sprintf("Interface should not have user group '%s' that product does not have", u)}
		} else {
			imSem.UserGroups[u] = rendering
		}
	}
	return nil
}

func (imSem *InterfaceMetadata) setObjectMatchers(cnf *Config, imSyn syntax.InterfaceMetadata, parent *InterfaceMetadata,
	dtaps syntax.Rendering) error {
	if imSyn.Objects == nil {
		if parent != nil {
			imSem.ObjectMatchers = parent.ObjectMatchers
			return nil
		}
		return &PolicyError{"ObjectMatcher is a required field for an interface"}
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

func (imSem *InterfaceMetadata) setHashColumns(cnf *Config, imSyn syntax.InterfaceMetadata, parent *InterfaceMetadata, ds DTAPSpec) error {
	if imSyn.MaskColumns == nil {
		if parent != nil {
			imSem.HashColumns = parent.HashColumns
		}
		return nil
	}
	if m, err := newColMatcher(cnf, imSyn.HashColumns, ds, imSem.UserGroups, imSem.ObjectMatchers); err != nil {
		return fmt.Errorf("hash_columns: %w", err)
	} else {
		imSem.HashColumns = m
	}
	return nil
}

func (imSem *InterfaceMetadata) setMaskColumns(cnf *Config, imSyn syntax.InterfaceMetadata, parent *InterfaceMetadata, ds DTAPSpec) error {
	if imSyn.MaskColumns == nil {
		if parent != nil {
			imSem.MaskColumns = parent.MaskColumns
		}
		return nil
	}
	if m, err := newColMatcher(cnf, imSyn.MaskColumns, ds, imSem.UserGroups, imSem.ObjectMatchers); err != nil {
		return fmt.Errorf("mask_columns: %w", err)
	} else {
		imSem.MaskColumns = m
	}
	return nil
}

func (imSem *InterfaceMetadata) setForProduct(imSyn syntax.InterfaceMetadata, parent *InterfaceMetadata) error {
	if imSyn.ForProduct == nil {
		if parent != nil {
			imSem.ForProduct = parent.ForProduct
		}
		return nil
	}
	imSem.ForProduct = imSyn.ForProduct
	return nil
}

func (lhs InterfaceMetadata) Equal(rhs InterfaceMetadata) bool {
	return lhs.ObjectMatchers.Equal(rhs.ObjectMatchers) &&
		lhs.Classification == rhs.Classification &&
		lhs.UserGroups.Equal(rhs.UserGroups) &&
		lhs.MaskColumns.Equal(rhs.MaskColumns) &&
		lhs.HashColumns.Equal(rhs.MaskColumns) &&
		maps.EqualFunc(lhs.ConsumedBy, rhs.ConsumedBy, func (l map[ProductDTAPID]struct{}, r map[ProductDTAPID]struct{}) bool { return maps.Equal(l, r) }) &&
		util.EqualStrPtr(lhs.ForProduct, rhs.ForProduct)
}
