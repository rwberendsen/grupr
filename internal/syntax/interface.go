package syntax

import (
	"fmt"
)

type Interface struct {
	ID		string `yaml:"id"`
	ProductID		string `yaml:"product_id"`
	InterfaceMetadata `yaml:"interface_metadata,inline"`
}

func (i * Interface) validate() error {
	if err := validateID(i.ID); err != nil { return err }
	if err := InterfaceMetadata.validate(); err != nil { return fmt.Errorf("interface '%s': %v", iid, err) }
	return nil
}
