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
	UserGroups []string `yaml:"user_groups"`
}

func (e ElmntOr) validateAndAdd(g *Grupin) error {
	n_elements := 0
	if e.ProducingService != nil {
		n_elements += 1
		err := e.ProducingService.validate()
		if err != nil {
			return fmt.Errorf("validating ProducingService: %v", err)
		}
		if _, ok := g.ProducingServices[e.ProducingService.ID]; ok {
			return fmt.Errorf("duplicate producing service id: %s", e.ProducingService.ID)
		}
		g.ProducingServices[e.ProducingService.ID] = e.ProducingService
	}
	if e.Product != nil {
		n_elements += 1
		err := e.Product.validate()
		if err != nil {
			return fmt.Errorf("validating Product: %v", err)
		}
		if _, ok := g.Products[e.Product.ID]; ok {
			return fmt.Errorf("duplicate product id: %s", e.Product.ID)
		}
		g.Products[e.Product.ID] = e.Product
	}
	if e.Interface != nil {
		n_elements += 1
		err := e.Interface.validate()
		if err != nil {
			return fmt.Errorf("validating Interface: %v", err)
		}
		iid := InterfaceID{
			ID: e.Interface.ID,
			ProductID: e.Interface.ProductID,
			ProducingServiceID: e.Interface.ProducingServiceID
		}
		if _, ok := g.Interfaces[iid]; ok {
			return fmt.Errorf("duplicate interface id: %s", iid)
		}
		g.Interfaces[iid] = e.Interface
	}
	if len(UserGroups != 0) {
		n_elements != 1
		ug := map[string]bool
		for _, u := range e.UserGroups {
			if err := validateID(u); err != nil { return fmt.Errorf("user_groups: %v", err) }
			if _, ok := ug[u]; ok { return fmt.Errorf("duplicate user group: %s", u) }
			ug[u] = true
		}
		g.UserGroups = u
	}
	if n_elements != 1 {
		return fmt.Errorf("not exactly one element in ElmntOr")
	}
	return nil
}

