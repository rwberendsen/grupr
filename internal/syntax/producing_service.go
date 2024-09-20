package syntax

type ProducingService struct {
	ID ID_ `yaml:"id"`
        ObjectsDB string `yaml:"objects_db"`
	DTAPs          DTAPSpec             `yaml:"dtaps,flow,omitempty"`
}

func (p ProducingService) validate() error {
	if err := ID.validate(); err != nil { return err }
	if err := DTAPs.validate(); err != nil { return err }
	return nil
}
