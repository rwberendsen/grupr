package syntax

import (
	"fmt"
)

type Interface struct {
	ID		ID_ `yaml:"id"`
	ProductID		ID_ `yaml:"product_id"`
	ProducingServiceID *ID_ `yaml:"producing_service_id,omitempty"`
	Classification string `yaml:",omitempty"` // inherits from product if not specified; can be less strict, but not stricter
	CanLeaveGroup  *bool                `yaml:"can_leave_group,omitempty"`
	ExposeDTAPs    []ID_
	UserGroups     []ID_            `yaml:"user_groups,flow,omitempty"`
	UserGroupColumn *string		    `yaml:"user_group_column,omitempty"`
	Objects        []string
	ObjectsExclude []string `yaml:"objects_exclude,omitempty"`
	MaskColumns    []string `yaml:"mask_columns,omitempty"`
	HashColumns    []string `yaml:"hash_columns,omitempty"`
}

func (i * Interface) validate() error {
	if err := i.ID.validate(); err != nil { return fmt.Errorf("interface %s: %v", i.ID, err) }
	if err := i.ProductID.validate(); err != nil { return fmt.Errorf("interface %s: %v", i.ID, err) }
	if i.ProducingServiceID != nil {
		if err := i.ProducingServiceID.validate(); err != nil { return fmt.Errorf("interface %s: %v", i.ID, err) }
	}
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
}
