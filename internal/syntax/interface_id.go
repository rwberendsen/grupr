package syntax

import (
	"fmt"
)

type InterfaceID struct {
	ID        string
	ProductID string `yaml:"product_id"`
}

func (iid InterfaceID) validate(cnf *Config) error {
	if err := validateIDPart(cnf, iid.ID); err != nil {
		return fmt.Errorf("ID: %w", err)
	}
	if err := validateIDPart(cnf, iid.ProductID); err != nil {
		return fmt.Errorf("ProductID: %w", err)
	}
	return nil
}
