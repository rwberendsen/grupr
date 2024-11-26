package syntax

import (
	"fmt"
)

type Product struct {
	ID	       string `yaml:"id"`
	DTAPs          DTAPSpec             `yaml:"dtaps,flow,omitempty"`
	DTAPRendering   Rendering 		`yaml:"dtap_rendering,omitempty"`
	Consumes       []InterfaceID   	    `yaml:",omitempty"`
	InterfaceMetadata	    `yaml:"product_interface,inline"`
}

func (p *Product) validate() error {
	if err := validateID(p.ID); err != nil { return err }
	if err := p.DTAPs.validate(); err != nil {
		return fmt.Errorf("product id: %s, DTAPs: %w", p.ID, err)
	}
	if err := p.DTAPRendering.validate(); err != nil { return fmt.Errorf("product '%s': DTAPRendering: %w", p.ID, err) }
	for d := range p.DTAPRendering {
		if !p.DTAPs.HasDTAP(d) { return &FormattingError{fmt.Sprintf("product '%s': DTAPRendering: unknown dtap '%s'", p.ID, d)} }
	}
	if err := p.InterfaceMetadata.validate(); err != nil { return fmt.Errorf("product %s: %w", p.ID, err) }
	return nil
}
