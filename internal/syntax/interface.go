package syntax

import (
	"fmt"
)

type Interface struct {
	ID                string `yaml:"id"`
	ProductID         string `yaml:"product_id"`
	InterfaceMetadata `yaml:",inline"`
}

func (i *Interface) validate(cnf *Config) error {
	if err := validateIDPart(cnf, i.ID); err != nil {
		return err
	}
	if err := validateIDPart(cnf, i.ProductID); err != nil {
		return err
	}
	if err := i.InterfaceMetadata.validate(cnf); err != nil {
		return fmt.Errorf("interface '%s' of product '%s': %v", i.ID, i.ProductID, err)
	}
	return nil
}
