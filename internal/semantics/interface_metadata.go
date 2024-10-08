package semantics

import (
	"fmt"
	"github.com/rwberendsen/grupr/internal/syntax"
)

type InterfaceMetadata struct {
	ObjectMatcher	ObjMatcher
	Classification Classification
	Usergroups map[string]bool
	UserGroupColumn ColMatcher
	MaskColumns ColMatcher
	HashColumns ColMatcher
	ExposeDTAPs map[string]bool
	DTAPRendering map[string]string // Renderings may contain upper-case characters, so they can be used inside quoted fields
	UserGroupRendering map[string]string // Renderings may contain upper-case characters, so they can be used inside quoted fields
}


func newInterfaceMetadata(imSyn syntax.InterfaceMetadata, allowedUserGroups map[string]bool, dtaps syntax.DTAPSpec, parent *InterfaceMetadata) (InterfaceMetadata, error) {
	// p *Product: if not nil, it will have already validated product-level interface metadata
	imSem := InterfaceMetadata{ExposeDTAPs: map[string]bool{}}
	if err := imSem.setClassification(imSyn, parent); err != nil { return err }
	if err := imSem.setUserGroups(imSyn, parent, allowedUserGroups); err != nil { return err }
	if err := imSem.setUserGroupColumn(imSyn, parent); err != nil { return err }
	if err := imSem.setExposeDTAPs(imSyn, parent, dtaps); err != nil { return err }
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
	ug := map[string]bool
	for _, u := range imSyn.UserGroups {
		if _, ok := allowedUserGroups[u]; !ok { return SetLogicError{fmt.Sprintf("Unknown user group: %s", u)} }
		if _, ok := ug[u]; ok { return SetLogicError{fmt.Sprintf("Duplicate user group: %s", u) } }
		ug[u] = true
	}
	imSem.UserGroups = ug
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
}

func (imSem *InterfaceMetadata) setExposeDTAPs(imSyn syntax.InterfaceMetadata, parent *InterfaceMetadata, dtaps syntax.DTAPSpec) error {
	if imSyn.ExposeDTAPs == nil {
		if parent != nil {
			imSem.ExposeDTAPs = parent.ExposeDTAPs
		}
		return nil
	}
	for _, d := range imSyn.ExposeDTAPs {
		if _, ok := imSem.ExposeDTAPs[d]; ok { return syntax.FormattingError{fmt.Sprintf("expose dtaps: duplicate dtap '%s'", d)}
		if !dtaps.HasDTAP(d) { return SetLogicError{fmt.Sprintf("expose dtaps: unknown dtap '%s'", d)}
		imSem.ExposeDTAPs[d] = true
	}
	return nil
}
