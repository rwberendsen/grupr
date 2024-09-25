package semantics

import (
	"fmt"
)

type InterfaceMetadata struct {
	Classification Classification_
	Usergroups []UserGroup
	UserGroupColumn ColumnMatcher
	Matcher
	MaskColumns ColumnMatcher
	HashColumns ColumnMatcher
	ExposeDTAPs []string
	DTAPRendering map[string]string
	UserGroupRendering map[string]string
}


func newInterfaceMetadata(im_syn syntax.InterfaceMetadata, productID string) (InterfaceMetadata, error) {
	im_sem := InterfaceMetadata{}
	if productID == "" {
		// this object is a product-level interface
		if Classification == "" {
			return fmt.Errorf("Classification required on product-level interface")
		}
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
	return im_sem
}
