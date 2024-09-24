package syntax

import (
	"fmt"
)

type Interface struct {
	ID		string `yaml:"id"`
	ProductID		string `yaml:"product_id"`
	ProducingServiceID string `yaml:"producing_service_id,omitempty"`
	InterfaceMetadata `yaml:"interface_metadata,inline"`
}

func (i * Interface) validate() error {
	iid := InterfaceID{ID: i.ID, ProductID: i.ProductID, ProducingServiceID: i.ProducingServiceID}
	if err := iid.validate(); err != nil { return fmt.Errorf("interface: %s") }
	if err := InterfaceMetadata.validate(); err != nil { return fmt.Errorf("interface '%s': %v", iid, err) }
	return nil
}
