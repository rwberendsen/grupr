package syntax

import (
	"fmt"
)

type InterfaceMetadata struct {
	Classification string
	CanLeaveGroup  *bool                `yaml:"can_leave_group,omitempty"`
	UserGroups     []string             `yaml:"user_groups,flow,omitempty"`
	UserGroupColumn string		    `yaml:"user_group_column,omitempty"`
	Objects        []string             `yaml:",omitempty"`
	ObjectsExclude []string             `yaml:"objects_exclude,omitempty"`
	MaskColumns	[]string	    `yaml:"mask_columns"`
	HashColumns	[]string	    `yaml:"hash_columns"`
	ExposeDTAPs	[]string	    `yaml:"expose_dtaps"`
}

func (i InterfaceMetadata) validate() error {
	if err := validateClassification(i.Classification, i.CanLeaveGroup); err != nil { return fmt.Errorf("interface %s: %v", i.ID, err) }
	for d := range i.ExposeDTAPs {
		if err := d.validate(); err != nil { return fmt.Errorf("interface %s: exposed DTAP %s: %v", i.ID, d, err) }
	}
	for u := range i.UserGroups {
		if err := d.validate(); err != nil { return fmt.Errorf("interface %s: usergroup %s: %v", i.ID, u, err) }
	}
	if i.UserGroupColumn != nil {
		if i.UserGroups == nil || len(i.UserGroups == 0 { return fmt.Errorf("interface %s: usergroup column specified but no user groups") }
	}
	if i.Objects == nil || len(i.Objects) {
		return fmt.Errorf("interface %s: no objects specified in interface")
	}
	return nil
	//if err := validateClassification(p.Classification, p.CanLeaveGroup); err != nil {
	//	return fmt.Errorf("product id: %s, Classificatoin: %v", p.ID, err)
	//}
	//for u := range p.UserGroups {
	//	if err := d.validate(); err != nil { return fmt.Errorf("product %s: usergroup %s: %v", p.ID, u, err) }
	//}
	//if p.UserGroupColumn != nil {
	//	if p.UserGroups == nil || len(p.Usergroups) == 0 { return fmt.Errorf("product %s: usergroup column specified but no user groups", p.ID) }
	//}
	//if (p.Objects == nil || len(p.Objects) == 0) && p.ObjectsExclude != nil {
	//	return fmt.Errorf("product %s: no objects specified, but objects to exclude were specified", p.ID)
	//}
	//for i := range p.Consumes {
	//	if err := i.validate(); err != nil { return fmt.Errorf("product %s: consumes: %v", p.ID, err) }
	//}
}
