package syntax

import (
	"fmt"
)

type ConsumptionSpec struct {
	InterfaceID       `yaml:",inline"`
	DTAPMapping       map[string]string `yaml:"dtap_mapping,omitempty"`
	NonConsumingDTAPs []string          `yaml:"non_consuming_dtaps,omitempty"`
}

func (cs ConsumptionSpec) validate(cnf *Config, ds DTAPSpec) error {
	if err := cs.InterfaceID.validate(cnf); err != nil {
		return err
	}
	for k, v := range cs.DTAPMapping {
		if !ds.HasDTAP(k) {
			return &FormattingError{fmt.Sprintf("dtap_mapping: '%s': unknown dtap", k)}
		}
		if err := ValidateIDPart(cnf, v); err != nil { // can't check if DTAP exists, belongs to other product, will happen in semantics package
			return err
		}
	}
	ncd := map[string]struct{}{}
	for _, k := range cs.NonConsumingDTAPs {
		if !ds.HasDTAP(k) {
			return &FormattingError{fmt.Sprintf("dtap_mapping: '%s': unknown dtap", k)}
		}
		if _, ok := ncd[k]; ok {
			return &FormattingError{fmt.Sprintf("non_consuming_dtaps: '%s': duplicate dtap", k)}
		}
		ncd[k] = struct{}{}
	}
	return nil
}
