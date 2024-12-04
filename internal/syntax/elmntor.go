package syntax

import (
	"fmt"
)


type ElmntOr struct {
	Classes map[string]Class
	AllowedUserGroups []string `yaml:"allowed_user_groups"`
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
	if e.AllowedUserGroups != nil {
		if g.AllowedUserGroups != nil {
			return &FormattingError{"allowed_user_groups specified more than once"}
		}
		n_elements += 1
		ug := map[string]bool{}
		for _, u := range e.AllowedUserGroups {
			if err := validateID(u); err != nil { return fmt.Errorf("user_groups: %w", err) }
			if _, ok := ug[u]; ok { return &FormattingError{fmt.Sprintf("duplicate user group: %s", u)} }
			ug[u] = true
		}
		g.AllowedUserGroups = ug
	}
	if e.Product != nil {
		n_elements += 1
		err := e.Product.validate()
		if err != nil {
			return err
		}
		if _, ok := g.Products[e.Product.ID]; ok {
			return &FormattingError{fmt.Sprintf("duplicate product id: %s", e.Product.ID)}
		}
		g.Products[e.Product.ID] = *e.Product
	}
	if e.Interface != nil {
		n_elements += 1
		err := e.Interface.validate()
		if err != nil {
			return err
		}
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

