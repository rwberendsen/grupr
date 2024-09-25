package syntax

type ProducingService struct {
	ID string `yaml:"id"`
        ObjectsDB string `yaml:"objects_db,omitempty"`
	DTAPs          DTAPSpec             `yaml:"dtaps,flow,omitempty"`
}

func (p ProducingService) validate() error {
	if err := ID.validate(); err != nil { return err }
	if err := DTAPs.validate(); err != nil { return err }
	return nil
}
