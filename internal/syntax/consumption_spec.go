package syntax

type ConsumptionSpec struct {
	InterfaceID `yaml:",inline"`
	DTAPMapping map[string]string `yaml:"dtap_mapping,omitempty"`
}

func (cs ConsumptionSpec) validate() error {
	if err := cs.InterfaceID.validate(); err != nil {
		return err
	}
	for k, v := range cs.DTAPMapping {
		if err := validateID(k); err != nil {
			return err
		}
		if err := validateID(v); err != nil {
			return err
		}
	}
	return nil
}
