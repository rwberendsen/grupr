package syntax

import (
	"fmt"
)

type InterfaceID struct {
	ID string `yaml:"interface"`
	ProductID   string `yaml:"product"`
	ProducingServiceID string `yaml:"producing_service,omitempty"`
}

func (i InterfaceID) validate() error {
	if err := validateID(i.ID); err != nil { return err }
	if err := validateID(i.ProductID); err != nil { return err }
	if i.ProducingServiceID != "" {
		if err := validateID(i.ProducingServiceID); err != nil { return err }
	}
	return nil
}
