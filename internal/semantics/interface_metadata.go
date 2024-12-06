package semantics

import (
	"fmt"
	"maps"

	"github.com/rwberendsen/grupr/internal/syntax"
)

type InterfaceMetadata struct {
	ObjectMatcher	ObjMatcher
	Classification Classification
	GlobalUserGroups map[string]bool
	UserGroupMapping string
	UserGroups syntax.Rendering
	UserGroupColumn ColMatcher
	MaskColumns ColMatcher
	HashColumns ColMatcher
	ExposeDTAPs map[string]bool
	DTAPRendering syntax.Rendering
        ForProduct *string
}


func newInterfaceMetadata(imSyn syntax.InterfaceMetadata, classes map[string]syntax.Class, globalUserGroups map[string]bool,
                          userGroupMappings map[string]UserGroupMapping, dtaps syntax.Rendering, parent *InterfaceMetadata) (InterfaceMetadata, error) {
	imSem := InterfaceMetadata{}
	if err := imSem.setClassification(imSyn, parent, classes); err != nil { return imSem, err }
	if err := imSem.setUserGroupMapping(imSyn, parent, userGroupMappings); err != nil { return imSem, err }
	if err := imSem.setUserGroups(imSyn, parent, globalUserGroups, userGroupMappings); err != nil { return imSem, err }
	if err := imSem.setExposeDTAPs(imSyn, parent, dtaps); err != nil { return imSem, err }
	if err := imSem.setObjectMatcher(imSyn, parent, dtaps); err != nil { return imSem, err }
	if err := imSem.setUserGroupColumn(imSyn, parent, dtaps); err != nil { return imSem, err }
	if err := imSem.setMaskColumns(imSyn, parent, dtaps); err != nil { return imSem, err }
	if err := imSem.setHashColumns(imSyn, parent, dtaps); err != nil { return imSem, err }
	if err := imSem.setForProduct(imSyn, parent, dtaps); err != nil { return imSem, err }
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

func (imSem *InterfaceMetadata) setUserGroupMapping(imSyn syntax.InterfaceMetadata, parent *InterfaceMetadata,
						    userGroupMappings map[string]UserGroupMapping) error {
	if imSyn.UserGroupMapping == "" {
		if parent != nil {
			imSem.UserGroupMapping = parent.UserGroupMapping
			return nil
		}
		return nil
	}
	if _, ok := userGroupMappings[imSyn.UserGroupMapping]; !ok {
		return &SetLogicError{fmt.Sprintf("Unknown user group mapping: '%s'", imSyn.UserGroupMapping)}
	}
	imSem.UserGroupMapping = imSyn.UserGroupMapping
	return nil
}

func getGlobalUserGroup(userGroup string, userGroupMapping string, globalUserGroups map[string]bool, userGroupMappings map[string]UserGroupMapping) (string, bool) {
	if userGroupMapping == "" {
		_, ok := globalUserGroups[userGroup];
		return userGroup, ok // userGroup is a global user group
	}
	globalUserGroup, ok := userGroupMappings[userGroupMapping][userGroup]
	return globalUserGroup, ok
}

func (imSem *InterfaceMetadata) setUserGroups(imSyn syntax.InterfaceMetadata, parent *InterfaceMetadata,
					      globalUserGroups map[string]bool, userGroupMappings map[string]UserGroupMapping) error {
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
		if g, ok := getGlobalUserGroup(u, imSem.UserGroupMapping, globalUserGroups, userGroupMappings); !ok {
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
		for u := range imSem.GlobalUserGroups {
			if _, ok := parent.GlobalUserGroups[u]; !ok { return &PolicyError{fmt.Sprintf("Interface should not have global user group '%s' that product does not have", u)} }
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

func (imSem *InterfaceMetadata) setObjectMatcher(imSyn syntax.InterfaceMetadata, parent *InterfaceMetadata,
                                                 dtaps syntax.Rendering) error {
	if imSyn.Objects == nil {
		if parent != nil {
			imSem.ObjectMatcher = parent.ObjectMatcher
			return nil
		}
		return &PolicyError{"ObjectMatcher is a required field"}
	}
	if m, err := newObjMatcher(imSyn.Objects, imSyn.ObjectsExclude, dtaps, imSem.UserGroups); err != nil {
		return fmt.Errorf("ObjectMatcher: %w", err)
	} else {
		imSem.ObjectMatcher = m
	}
	if parent != nil {
		if !imSem.ObjectMatcher.subsetOf(parent.ObjectMatcher) {
			return &PolicyError{"ObjectMatcher should be a subset of parent ObjectMatcher"}
		}
	}
	return nil
}

func (imSem *InterfaceMetadata) setUserGroupColumn(imSyn syntax.InterfaceMetadata, parent *InterfaceMetadata,
                                                   dtaps syntax.Rendering) error {
	if imSyn.UserGroupColumn == "" {
		if parent != nil {
			imSem.UserGroupColumn = parent.UserGroupColumn
		}
		return nil
	}
	if m, err := newColMatcher([]string{imSyn.UserGroupColumn}, dtaps, imSem.UserGroups, imSem.ObjectMatcher); err != nil {
		return fmt.Errorf("user_group_column: %w", err)
	} else {
		imSem.UserGroupColumn = m
	}
	return nil
}

func (imSem *InterfaceMetadata) setHashColumns(imSyn syntax.InterfaceMetadata, parent *InterfaceMetadata, dtaps syntax.Rendering) error {
	if imSyn.MaskColumns == nil {
		if parent != nil {
			imSem.HashColumns = parent.HashColumns
		}
		return nil
	}
	if m, err := newColMatcher(imSyn.HashColumns, dtaps, imSem.UserGroups, imSem.ObjectMatcher); err != nil {
		return fmt.Errorf("hash_columns: %w", err)
	} else {
		imSem.HashColumns = m
	}
	return nil
}

func (imSem *InterfaceMetadata) setMaskColumns(imSyn syntax.InterfaceMetadata, parent *InterfaceMetadata, dtaps syntax.Rendering) error {
	if imSyn.MaskColumns == nil {
		if parent != nil {
			imSem.MaskColumns = parent.MaskColumns
		}
		return nil
	}
	if m, err := newColMatcher(imSyn.MaskColumns, dtaps, imSem.UserGroups, imSem.ObjectMatcher); err != nil {
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
		if lhs == nil || rhs == nil { return false }
		if *lhs != *rhs { return false }
	}
	return true
}

func (lhs InterfaceMetadata) Equal(rhs InterfaceMetadata) bool {
	return lhs.ObjectMatcher.Equal(rhs.ObjectMatcher) &&
	       lhs.Classification == rhs.Classification &&
	       maps.Equal(lhs.GlobalUserGroups, rhs.GlobalUserGroups) &&
	       lhs.UserGroupMapping == rhs.UserGroupMapping &&
	       lhs.UserGroups.Equal(rhs.UserGroups) &&
	       lhs.UserGroupColumn.Equal(rhs.UserGroupColumn) &&
	       lhs.MaskColumns.Equal(rhs.MaskColumns) &&
	       lhs.HashColumns.Equal(rhs.MaskColumns) &&
	       maps.Equal(lhs.ExposeDTAPs, rhs.ExposeDTAPs) &&
	       lhs.DTAPRendering.Equal(rhs.DTAPRendering) &&
	       equal_pointer_string(lhs.ForProduct, rhs.ForProduct)
}
