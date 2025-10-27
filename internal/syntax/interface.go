package syntax

import (
	"fmt"
)

type Interface struct {
	ID                string `yaml:"id"`
	ProductID         string `yaml:"product_id"`
	InterfaceMetadata `yaml:",inline"`
}

func (i *Interface) validate() error {
	if err := validateID(i.ID); err != nil {
		return err
	}
	if err := i.InterfaceMetadata.validate(); err != nil {
		return fmt.Errorf("interface '%s' of product '%s': %v", i.ID, i.ProductID, err)
	}
	return nil
}
