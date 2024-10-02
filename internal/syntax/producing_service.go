package syntax

import (
	"fmt"
)

type ProducingService struct {
	ID string `yaml:"id"`
        ObjectsDB string `yaml:"objects_db,omitempty"`
	DTAPs          DTAPSpec             `yaml:"dtaps,flow,omitempty"`
}

func (p ProducingService) validate() error {
	if err := validateID(); err != nil { return err }
	if err := DTAPs.validate(); err != nil { return fmt.Errorf("Producing service id '%s', DTAPs: %w", p.ID, err) }
	return nil
}
