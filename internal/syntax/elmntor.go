package syntax

import (
	"io"
	"fmt"

	"gopkg.in/yaml.v3"
)


type ElmntOr struct {
	ProducingService *ProducingService `yaml:"producing_service,omitempty"`
	Product *Product `yaml:",omitempty"`
	Interface *Interface`yaml:"interface,omitempty"`
	AllowedUserGroups []string `yaml:"allowed_user_groups"`
}

func (e ElmntOr) validateAndAdd(g *Grupin) error {
	n_elements := 0
	if e.ProducingService != nil {
		n_elements += 1
		err := e.ProducingService.validate()
		if err != nil {
			return err
		}
		if _, ok := g.ProducingServices[e.ProducingService.ID]; ok {
			return FormattingError{fmt.Sprintf("duplicate producing service id: %s", e.ProducingService.ID)}
		}
		g.ProducingServices[e.ProducingService.ID] = e.ProducingService
	}
	if e.Product != nil {
		n_elements += 1
		err := e.Product.validate()
		if err != nil {
			return err
		}
		if _, ok := g.Products[e.Product.ID]; ok {
			return FormattingError{fmt.Sprintf("duplicate product id: %s", e.Product.ID)}
		}
		g.Products[e.Product.ID] = e.Product
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
			ProducingServiceID: e.Interface.ProducingServiceID
		}
		if _, ok := g.Interfaces[iid]; ok {
			return FormattingError{fmt.Sprintf("duplicate interface id: %s", iid)}
		}
		g.Interfaces[iid] = e.Interface
	}
	if e.AllowedUserGroups != nil {
		if g.AllowedUserGroups != nil {
			return FormattingError{"allowed_user_groups specified more than once"}
		}
		n_elements += 1
		ug := map[string]bool
		for _, u := range e.AllowedUserGroups {
			if err := validateID(u); err != nil { return fmt.Errorf("user_groups: %w", err) }
			if _, ok := ug[u]; ok { return FormattingError{fmt.Sprintf("duplicate user group: %s", u)} }
			ug[u] = true
		}
		g.AllowedUserGroups = u
	}
	if n_elements != 1 {
		return FormattingError{"not exactly one element in ElmntOr"}
	}
	return nil
}

