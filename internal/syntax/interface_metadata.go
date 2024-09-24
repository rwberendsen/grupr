package syntax

import (
	"fmt"
)

type InterfaceMetadata struct {
	Classification *string `yaml:",omitempty"`
	CanLeaveGroup  *bool                `yaml:"can_leave_group,omitempty"`
	UserGroups     []string             `yaml:"user_groups,flow,omitempty"`
	UserGroupColumn *string		    `yaml:"user_group_column,omitempty"`
	Objects        []string             `yaml:",omitempty"`
	ObjectsExclude []string             `yaml:"objects_exclude,omitempty"`
	MaskColumns	[]string	    `yaml:"mask_columns,omitempty"`
	HashColumns	[]string	    `yaml:"hash_columns,omitempty"`
	ExposeDTAPs	[]string	    `yaml:"expose_dtaps,flow,omitempty"`
}

func (i InterfaceMetadata) validate() error {
	if Classification != nil {
		if err := validateClassification(i.Classification, i.CanLeaveGroup); err != nil { return err }
	} else if CanLeaveGroup != nil {
		return fmt.Errorf("Classification not specified but CanLeaveGroup was specified")
	}
	for u := range i.UserGroups {
		if err := validateID(u); err != nil { return fmt.Errorf("UserGroup %s: %v", u, err) }
	}
	if i.UserGroupColumn != nil {
		if i.UserGroups == nil || len(i.UserGroups == 0 { return fmt.Errorf("UserGroupColumn specified but not UserGroups") }
	}
	if len(i.Objects) == 0 && len(i.ObjectsExclude) != 0 {
		return fmt.Errorf("no objects specified, but objects to exclude were specified", p.ID)
	}
	for d := range i.ExposeDTAPs {
		if err := validateID(d); err != nil { return fmt.Errorf("exposed DTAPs: %v", err) }
	}
	return nil
}
