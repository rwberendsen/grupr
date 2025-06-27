package syntax

import (
	"fmt"
)

type InterfaceID struct {
	ID        string
	ProductID string `yaml:"product_id"`
}

func (iid InterfaceID) validate() error {
	if err := validateID(iid.ID); err != nil {
		return fmt.Errorf("ID: %w", err)
	}
	if err := validateID(iid.ProductID); err != nil {
		return fmt.Errorf("ProductID: %w", err)
	}
	return nil
}
