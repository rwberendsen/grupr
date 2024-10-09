package semantics

import (
	"fmt"
	"github.com/rwberendsen/grupr/internal/syntax"
)

type InterfaceMetadata struct {
	ObjectMatcher	ObjMatcher
	Classification Classification
	Usergroups syntax.Rendering
	UserGroupColumn ColMatcher
	MaskColumns ColMatcher
	HashColumns ColMatcher
	ExposeDTAPs map[string]bool
	DTAPRendering syntax.Rendering
}


func newInterfaceMetadata(imSyn syntax.InterfaceMetadata, allowedUserGroups map[string]bool, dtaps syntax.Rendering, parent *InterfaceMetadata) (InterfaceMetadata, error) {
	imSem := InterfaceMetadata{}
	if err := imSem.setClassification(imSyn, parent); err != nil { return err }
	if err := imSem.setUserGroups(imSyn, parent, allowedUserGroups); err != nil { return err }
	if err := imSem.setExposeDTAPs(imSyn, parent, dtaps); err != nil { return err }
	if err := imSem.setUserGroupRendering(imSyn, parent); err != nil { return err }
	if err := imSem.setObjectMatcher(imSyn, parent, dtaps); err != nil { return err }
	if err := imSem.setUserGroupColumn(imSyn, parent); err != nil { return err }
	// ...
	return imSem, nil
}

func (imSem *InterfaceMetadata) setClassification(imSyn syntax.InterfaceMetadata, parent *InterfaceMetadata) error {
	if imSyn.Classification == "" {
		if parent != nil {
			imSem.Classification = p.Classification
			return nil
		}
		return PolicyError{"Classfication is a required field on product level"}
	}
	imSem.Classification = newClassification(imSyn.Classification)
	if parent != nil && parent.Classification < imSem.Classification {
		return PolicyError{"Classification on interface higher than product classification"}
	}
	return nil
}

func (imSem *InterfaceMetadata) setUserGroups(imSyn syntax.InterfaceMetadata, parent *InterfaceMetadata, allowedUserGroups map[string]bool) error {
	if imSyn.UserGroups == nil {
		if parent != nil {
			imSem.UserGroups = parent.UserGroups
		}
		return nil
	}
	imSem.UserGroups = syntax.Rendering{}
	for _, u := range imSyn.UserGroups {
		if _, ok := allowedUserGroups[u]; !ok { return SetLogicError{fmt.Sprintf("Unknown user group: %s", u)} }
		if _, ok := imSem.UserGroups[u]; ok { return SetLogicError{fmt.Sprintf("Duplicate user group: %s", u) } }
		imSem.UserGroups[u] = u
	}
	for u, r := range imSyn.UserGroupRendering {
		if _, ok := imSem.UserGroups[u]; !ok { return SetLogicError{fmt.Sprintf("UserGroupRendering: unknown user group '%s'", u)} }
		imSem.UserGroups[u] = r
	}
	if parent != nil {
		for u := range imSem.UserGroups {
			if _, ok := parent.UserGroups[u]; !ok { return PolicyError{fmt.Sprintf("Interface should not have user group '%s' that product does not have", u)} }
		}
	}
	return nil
}

func (imSem *InterfaceMetadata) setExposeDTAPs(imSyn syntax.InterfaceMetadata, parent *InterfaceMetadata, dtaps syntax.Rendering) error {
	if imSyn.ExposeDTAPs == nil {
		if parent != nil {
			imSem.ExposeDTAPs = parent.ExposeDTAPs
		}
		return nil
	}
	imSem.ExposeDTAPs = make(map[string]bool, len(dtaps))
	for _, d := range imSyn.ExposeDTAPs {
		if _, ok := imSem.ExposeDTAPs[d]; ok { return syntax.FormattingError{fmt.Sprintf("ExposeDTAPs: duplicate dtap '%s'", d)}
		if _, ok := dtaps[d]; !ok { return SetLogicError{fmt.Sprintf("ExposeDTAPs: unknown dtap '%s'", d)}
		imSem.ExposeDTAPs[d] = true
	}
	return nil
}

func (imSem *InterfaceMetadata) setObjectMatcher(imSyn syntax.InterfaceMetadata, parent *InterfaceMetadata, dtaps syntax.Rendering) error {
	if imSyn.Objects == nil {
		if parent != nil {
			imSem.ObjectMatcher = parent.ObjectMatcher
			return nil
		}
		return PolicyError{"ObjectMatcher is a required field"}
	}
	if m, err := newObjMatcher(pSyn.Objects, pSyn.ObjectsExclude, dtaps, imSem.UserGroups); err != nil {
		return pSem, fmt.Errorf("ObjectMatcher: %w", err)
	pSem.ObjectMatcher = m
	if parent != nil {
		if !pSem.ObjectMatcher.subsetOf(parent.ObjectMatcher) {
			return PolicyError{"ObjectMatcher should be a subset of parent ObjectMatcher"}
		}
	}
	return nil
}

func (imSem *InterfaceMetadata) setUserGroupColumn(imSyn syntax.InterfaceMetadata, parent *InterfaceMetadata) error {
	if imSyn.UserGroupColumn == "" {
		if parent != nil {
			imSem.UserGroupColumn = parent.UserGroupColumn
		}
		return nil
	}
	if columnMatcher, err := newColMatcher(imSyn.UserGroupColumn); err != nil { // TODO colMatcher needs some context, like DTAPs and UserGroups
		return fmt.Errorf("user_group_column: %w", err)
	} else {
		imSem.UserGroupColumn = columnMatcher
	}
	return nil
}
