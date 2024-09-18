package syntax

import (
	"fmt"
)

type Product struct {
	Id	       ID
	Classification string
	CanLeaveGroup  *bool                `yaml:"can_leave_group,omitempty"`
	DTAPs          DTAPSpec             `yaml:"dtaps,flow,omitempty"`
	UserGroups     []string             `yaml:"user_groups,flow,omitempty"`
	UserGroupColumn string		    `yaml:"user_group_column,omitempty"`
	Objects        []string             `yaml:",omitempty"`
	ObjectsExclude []string             `yaml:"objects_exclude,omitempty"`
	Consumes       []InterfaceId   	    `yaml:",omitempty"`
	MaskColumns	[]string	    `yaml:"mask_columns"`
}

func (p *Product) validate() error {
	if err := Id.validate(); err != nil { return err }
	if err := validateClassification(Classification, CanLeaveGroup); err != nil {
		return fmt.Errorf("product id: %s, Classificatoin: %v", Id, err)
	}
	if err := DTAPs.validate(); err != nil {
		return fmt.Errorf("product id: %s, DTAPs: %v", Id, err)
	}
	return nil
}
