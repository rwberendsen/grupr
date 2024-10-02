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


func newInterfaceMetadata(imSyn syntax.InterfaceMetadata, allowedUserGroups map[string]bool, p *Product) (InterfaceMetadata, error) {
	// p *Product: if not nil, it will have already validated product-level interface metadata
	imSem := InterfaceMetadata{}
	if err := imSem.setClassification(imSyn, p); err != nil { return err }
	if err := imSem.setUserGroups(imSyn, allowedUserGroups, p); err != nil { return err }
	if err := imSem.setUserGroupColumn(imSyn, p); err != nil { return err }
	// ...
	return imSem, nil
}

func (imSem *InterfaceMetadata) setClassification(imSyn syntax.InterfaceMetadata, p *Product) error {
	if imSyn.Classification == "" {
		if p != nil {
			imSem.Classification = p.Classification
			return nil
		}
		return PolicyError{"Classfication is a required field on product level"}
	}
	imSem.Classification = newClassification(imSyn.Classification)
	if p != nil && p.Classification < imSem.Classification {
		return PolicyError{"Classification on interface higher than product classification"}
	}
	return nil
}

func (imSem *InterfaceMetadata) setUserGroups(imSyn syntax.InterfaceMetadata, allowedUserGroups map[string]bool,  p *Product) error {
	if imSyn.UserGroups == nil {
		imSem.UserGroups = p.UserGroups
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

func (imSem *InterfaceMetadata) setUserGroupColumn(imSyn syntax.InterfaceMetadata, p *Product) error {
	if imSyn.UserGroupColumn == nil {
		imSem.UserGroupColumn = p.UserGroupColumn
		return nil
	}
	if columnMatcher, err := newColMatcher(imSyn.UserGroupColumn); err != nil {
		return fmt.Errorf("user_group_column: %w", err)
	} else {
		imSem.UserGroupColumn = columnMatcher
	}
}
