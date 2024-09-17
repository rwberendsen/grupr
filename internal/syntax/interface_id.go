package syntax

type InterfaceId struct {
	ProductId   string `yaml:"product"`
	InterfaceId string `yaml:"interface"`
	ProducingServiceId string `yaml:"producing_service,omitempty"`
}
