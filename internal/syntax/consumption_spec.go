package syntax

type ConsumptionSpec struct {
	InterfaceID `yaml:",inline"`
	DTAPMapping map[string]string `yaml:"dtap_mapping,omitempty"`
}

func (cs ConsumptionSpec) validate(cnf *Config) error {
	if err := cs.InterfaceID.validate(cnf); err != nil {
		return err
	}
	for k, v := range cs.DTAPMapping {
		if err := ValidateID(k); err != nil {
			return err
		}
		if err := ValidateID(v); err != nil {
			return err
		}
	}
	return nil
}
