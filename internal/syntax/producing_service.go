package syntax

import (
	"fmt"
)

type ProducingService struct {
	ID string `yaml:"id"`
        ObjectsDB string `yaml:"objects_db,omitempty"`
	DTAPs          DTAPSpec             `yaml:"dtaps,flow,omitempty"`
	DTAPRendering   Rendering 		`yaml:"dtap_rendering,omitempty"`
}

func (s ProducingService) validate() error {
	if err := validateID(); err != nil { return err }
	if err := s.DTAPs.validate(); err != nil {
		return fmt.Errorf("producing service id: %s, DTAPs: %w", s.ID, err)
	}
	if err := s.DTAPRendering.validate(); err != nil { return fmt.Errorf("product '%s': DTAPRendering: %w", s.ID, err) }
	for d := range s.DTAPRendering {
		if !s.DTAPs.HasDTAP(d) { return FormattingError{fmt.Sprintf("product '%s': DTAPRendering: unknown dtap '%s'", s.ID, d)} }
	}
	return nil
}
