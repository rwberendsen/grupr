package semantics

import (
	"fmt"
	"github.com/rwberendsen/grupr/internal/syntax"
)

type InterfaceMetadata struct {
	Classification Classification
	Usergroups []string
	UserGroupColumn ColumnMatcher
	MaskColumns ColumnMatcher
	HashColumns ColumnMatcher
	ExposeDTAPs []string
	DTAPRendering map[string]string
	UserGroupRendering map[string]string
}


func newInterfaceMetadata(imSyn syntax.InterfaceMetadata, ug syntax.UserGroups, p *Product) (InterfaceMetadata, error) {
	imSem := InterfaceMetadata{}
	if p != nil {
		if imSyn.Classification == "" {
			imSem.Classification = p.Classification
		} else {
			if c, err := newClassification(imSyn.Classification); err != nil {
				return imSem, err
			}
		}
		if imSyn.Usergroups == nil { imSem.UserGroups = p.UserGroups }
		if imSyn.UserGroupColumn == "" { imSem.UserGroupColumn = p.UserGroupColumn }
		if imSyn.MaskColumns == nil { imSem.MaskColumns = p.MaskColumns }
 		if imSyn.HashColumns == nil { imSem.HashColumns = p.HashColumns }
		if imSyn.ExposeDTAPs == nil { imSem.ExposeDTAPs = p.ExposeDTAPs }
		if imSyn.DTAPRendering == nil { imSem.DTAPRendering = p.DTAPRendering }
		if imSyn.UserGroupRendering == nil { imSem.UserGroupRendering = p.UserGroupRendering }
	}
	if p == nil {
		// the interface metadata is product-level
		if imSyn.Classification == "" {
			return fmt.Errorf("Classification required on product-level")
		}
		imSem.Classification = newClassification(imSyn.Classification)
		if err := validateUserGroups(imSyn.UserGroups, ug); err != nil { return imSem, err }
		imSem.UserGroups = imSyn.UserGroups
	} else {
		// the interface metadata should be consistent with the product level interface metadata
	}
	// TODO copy pasted from old newProduct function, to check
	for _, i := range p_syn.UserGroups {
		if _, ok := p_sem.UserGroups[i]; ok {
			return p_sem, fmt.Errorf("duplicate user group")
		}
		p_sem.UserGroups[i] = true
	}
	if m, err := newMatcher(p_syn.Objects, p_syn.ObjectsExclude, p_sem.DTAPs, p_sem.UserGroups); err != nil {
		return p_sem, fmt.Errorf("invalid object matching expressions: %s", err)
	} else {
		p_sem.Matcher = m
	}
	for k, v := range p_syn.Interfaces {
		if !validId.MatchString(k) {
			return p_sem, fmt.Errorf("invalid interface id: '%s'", k)
		}
		if i, err := newInterface(v, p_sem.DTAPs, p_sem.UserGroups); err != nil {
			return p_sem, fmt.Errorf("invalid interface '%s': %s", k, err)
		} else {
			p_sem.Interfaces[k] = i
		}
	}
	return imSem
}
