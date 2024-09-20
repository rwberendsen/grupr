package syntax

import (
	"fmt"
)

type Product struct {
	ID	       ID_ `yaml:"id"`
	Classification string
	CanLeaveGroup  *bool                `yaml:"can_leave_group,omitempty"`
	DTAPs          DTAPSpec             `yaml:"dtaps,flow,omitempty"`
	UserGroups     []string             `yaml:"user_groups,flow,omitempty"`
	UserGroupColumn *string		    `yaml:"user_group_column,omitempty"`
	Objects        []string             `yaml:",omitempty"`
	ObjectsExclude []string             `yaml:"objects_exclude,omitempty"`
	Consumes       []InterfaceId   	    `yaml:",omitempty"`
	MaskColumns	[]string	    `yaml:"mask_columns"`
}

func (p *Product) validate() error {
	if err := p.ID.validate(); err != nil { return err }
	if err := validateClassification(p.Classification, p.CanLeaveGroup); err != nil {
		return fmt.Errorf("product id: %s, Classificatoin: %v", p.ID, err)
	}
	if err := p.DTAPs.validate(); err != nil {
		return fmt.Errorf("product id: %s, DTAPs: %v", p.ID, err)
	}
	for u := range p.UserGroups {
		if err := d.validate(); err != nil { return fmt.Errorf("product %s: usergroup %s: %v", p.ID, u, err) }
	}
	if p.UserGroupColumn != nil {
		if p.UserGroups == nil || len(p.Usergroups) == 0 { return fmt.Errorf("product %s: usergroup column specified but no user groups", p.ID) }
	}
	if (p.Objects == nil || len(p.Objects) == 0) && p.ObjectsExclude != nil {
		return fmt.Errorf("product %s: no objects specified, but objects to exclude were specified", p.ID)
	}
	for i := range p.Consumes {
		if err := i.validate(); err != nil { return fmt.Errorf("product %s: consumes: %v", p.ID, err) }
	}
	return nil
}
