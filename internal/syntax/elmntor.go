package syntax

import (
	"fmt"
)

type ElmntOr struct {
	Classes          map[string]Class
	GlobalUserGroups *GlobalUserGroups `yaml:"global_user_groups,omitempty"`
	UserGroupMapping *UserGroupMapping `yaml:"user_group_mapping,omitempty"`
	Product          *Product          `yaml:",omitempty"`
	Interface        *Interface        `yaml:"interface,omitempty"`
	ServiceAccount   *ServiceAccount   `yaml:"service_account,omitempty"`
	Team             *Team             `yaml:"team,omitempty"`
}

func (e ElmntOr) validateAndAdd(g *Grupin) error {
	nElements := 0
	if e.Classes != nil {
		if g.Classes != nil {
			return &FormattingError{"classes specified more than once"}
		}
		nElements += 1
		for k, v := range e.Classes {
			if err := v.validate(); err != nil {
				return fmt.Errorf("classes: class id '%s': %w", k, err)
			}
		}
		g.Classes = e.Classes
	}
	if e.GlobalUserGroups != nil {
		nElements += 1
		if g.GlobalUserGroups != nil {
			return &FormattingError{"user_groups specified more than once"}
		}
		g.GlobalUserGroups = e.GlobalUserGroups
	}
	if e.UserGroupMapping != nil {
		nElements += 1
		if _, ok := g.UserGroupMappings[e.UserGroupMapping.ID]; ok {
			return &FormattingError{fmt.Sprintf("duplicate user group mapping: '%s'", e.UserGroupMapping.ID)}
		}
		g.UserGroupMappings[e.UserGroupMapping.ID] = *e.UserGroupMapping
	}
	if e.Product != nil {
		nElements += 1
		if err := e.Product.validate(); err != nil {
			return err
		}
		if _, ok := g.Products[e.Product.ID]; ok {
			return &FormattingError{fmt.Sprintf("duplicate product id: %s", e.Product.ID)}
		}
		g.Products[e.Product.ID] = *e.Product
	}
	if e.Interface != nil {
		nElements += 1
		iid := InterfaceID{
			ID:        e.Interface.ID,
			ProductID: e.Interface.ProductID,
		}
		if _, ok := g.Interfaces[iid]; ok {
			return &FormattingError{fmt.Sprintf("duplicate interface id: %s", iid)}
		}
		g.Interfaces[iid] = *e.Interface
	}
	if e.ServiceAccount != nil {
		nElements += 1
		if err := e.ServiceAccount.validate(); err != nil {
			return err
		}
		if _, ok := g.ServiceAccounts[e.ServiceAccount.ID]; ok {
			return &FormattingError{fmt.Sprintf("duplicate service account id")}
		}
		g.ServiceAccounts[e.ServiceAccount.ID] = *e.ServiceAccount
	}
	if e.Team != nil {
		nElements += 1
		if _, ok := g.Teams[e.Team.ID]; ok {
			return &FormattingError{fmt.Sprintf("duplicate team id")}
		}
		g.Teams[e.Team.ID] = *e.Team
	}
	if nElements != 1 {
		return &FormattingError{"not exactly one element in ElmntOr"}
	}
	return nil
}
