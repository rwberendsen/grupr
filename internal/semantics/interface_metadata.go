package semantics

import (
	"fmt"
	"github.com/rwberendsen/grupr/internal/syntax"
)

type InterfaceMetadata struct {
	Classification Classification
	Usergroups map[string]bool
	UserGroupColumn ColumnMatcher
	MaskColumns ColumnMatcher
	HashColumns ColumnMatcher
	ExposeDTAPs map[string]bool
	DTAPRendering map[string]string
	UserGroupRendering map[string]string
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
		return fmt.Errorf("Classfication is a required field on product level")
	}
	imSem.Classification = newClassification(imSyn.Classification)
	return nil
}

func (imSem *InterfaceMetadata) setUserGroups(imSyn syntax.InterfaceMetadata, allowedUserGroups map[string]bool,  p *Product) error {
	if imSyn.UserGroups == nil {
		imSem.UserGroups = p.UserGroups
		return nil
	}
	ug := map[string]bool
	for _, u := range imSyn.UserGroups {
		if _, ok := allowedUserGroups[u]; !ok { return fmt.Errorf("Unknown user group: %s", u) }
		if _, ok := ug[u]; ok { return fmt.Errorf("Duplicate user group: %s", u) }
		ug[u] = true
	}
	imSem.UserGroups = ug
}

func (imSem *InterfaceMetadata) setUserGroupColumn(imSyn syntax.InterfaceMetadata, p *Product) error {
	if imSyn.UserGroupColumn == nil {
		imSem.UserGroupColumn = p.UserGroupColumn
		return nil
	}
	if columnMatcher, err := newColumnMatcher(imSyn.UserGroupColumn); err != nil {
		return fmt.Errorf("user_group_column: %v", err)
	} else {
		imSem.UserGroupColumn = columnMatcher
	}
}
