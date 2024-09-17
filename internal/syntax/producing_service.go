package syntax

type ProducingService struct {
	Id string
        ObjectsDB string `yaml:"objects_db"`
	DTAPs          DTAPSpec             `yaml:"dtaps,flow,omitempty"`
}
