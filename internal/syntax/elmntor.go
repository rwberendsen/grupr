package syntax

import (
	"fmt"
)


type ElmntOr struct {
	Classes map[string]Class
	GlobalUserGroups *GlobalUserGroups `yaml:"global_user_groups,omitempty"`
	UserGroupMapping *UserGroupMapping `yaml:"user_group_mapping,omitempty"`
	Product *Product `yaml:",omitempty"`
	Interface *Interface`yaml:"interface,omitempty"`
}

func (e ElmntOr) validateAndAdd(g *Grupin) error {
	n_elements := 0
	if e.Classes != nil {
		if g.Classes != nil {
			return &FormattingError{"classes specified more than once"}
		}
		n_elements += 1
		for k, v := range e.Classes {
			if err := validateID(k); err != nil { return fmt.Errorf("classes: invalid class id: '%s'", k) }
			if err := v.validate(); err != nil { return fmt.Errorf("classes: class id '%s': %w", k, err) }
		}
		g.Classes = e.Classes
	}
	if e.GlobalUserGroups != nil {
		if g.GlobalUserGroups != nil {
			return &FormattingError{"user_groups specified more than once"}
		}
		if err := e.GlobalUserGroups.validate(); err != nil { return err }
		n_elements += 1
		g.GlobalUserGroups = e.GlobalUserGroups
	}
	if e.UserGroupMapping != nil {
		n_elements += 1
		if err := e.UserGroupMapping.validate(); err != nil { return err }
		if _, ok := g.UserGroupMappings[e.UserGroupMapping.ID]; ok {
			return &FormattingError{fmt.Sprintf("duplicate user group mapping: '%s'", e.UserGroupMapping.ID)}
		}
		g.UserGroupMappings[e.UserGroupMapping.ID] = *e.UserGroupMapping
	}
	if e.Product != nil {
		n_elements += 1
		if err := e.Product.validate(); err != nil { return err }
		if _, ok := g.Products[e.Product.ID]; ok {
			return &FormattingError{fmt.Sprintf("duplicate product id: %s", e.Product.ID)}
		}
		g.Products[e.Product.ID] = *e.Product
	}
	if e.Interface != nil {
		n_elements += 1
		if err := e.Interface.validate(); err != nil { return err }
		iid := InterfaceID{
			ID: e.Interface.ID,
			ProductID: e.Interface.ProductID,
		}
		if _, ok := g.Interfaces[iid]; ok {
			return &FormattingError{fmt.Sprintf("duplicate interface id: %s", iid)}
		}
		g.Interfaces[iid] = *e.Interface
	}
	if n_elements != 1 {
		return &FormattingError{"not exactly one element in ElmntOr"}
	}
	return nil
}

