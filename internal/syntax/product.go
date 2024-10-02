package syntax

import (
	"fmt"
)

type Product struct {
	ID	       string `yaml:"id"`
	DTAPs          DTAPSpec             `yaml:"dtaps,flow,omitempty"`
	Consumes       []InterfaceId   	    `yaml:",omitempty"`
	InterfaceMetadata	    `yaml:"product_interface,inline"`
}

func (p *Product) validate() error {
	if err := validateID(p.ID); err != nil { return err }
	if err := p.DTAPs.validate(); err != nil {
		return fmt.Errorf("product id: %s, DTAPs: %w", p.ID, err)
	}
	if err := InterfaceMetadata.validate(); err != nil { return fmt.Errorf("product %s: %w", p.ID, err) }
	return nil
}
