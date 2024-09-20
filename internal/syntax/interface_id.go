package syntax

type InterfaceID struct {
	ProductID   ID_ `yaml:"product"`
	InterfaceID ID_ `yaml:"interface"`
	ProducingServiceID ID_ `yaml:"producing_service,omitempty"`
}

func (i InterfaceID) validate() error {
	if err := i.ProductID.validate(); err != nil { return err }
	if err := i.InterfaceID.validate(); err != nil { return err }
	if err := i.ProducingServiceID.validate(); err != nil { return err }
	return nil
}
