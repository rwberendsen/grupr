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
}

func (e ElmntOr) validate() error {
	n_elements := 0
	if e.ProducingService != nil {
		n_elements += 1
		err := e.ProducingService.validate()
		if err != nil {
			return fmt.Errorf("validating ProducingService: %v", err)
		}
	}
	if e.Product != nil {
		n_elements += 1
		err := e.Product.validate()
		if err != nil {
			return fmt.Errorf("validating Product: %v", err)
		}
	}
	if e.Interface != nil {
		n_elements += 1
		err := e.Interface.validate()
		if err != nil {
			return fmt.Errorf("validating Interface: %v", err)
		}
	}
	if n_elements != 1 {
		return fmt.Errorf("not exactly one element in ElmntOr")
	}
	return nil
}

