package syntax

import (
	"fmt"
)

type DTAPSpec struct {
	NonProd []string `yaml:"non_prod,flow,omitempty"`
	Prod    *string  `yaml:",omitempty"`
}

func (d DTAPSpec) validate(cnf *Config) error {
	dtaps := map[string]bool{}
	if d.Prod != nil {
		if err := ValidateIDPart(cnf, *d.Prod); err != nil {
			return fmt.Errorf("prod DTAP id: %w", err)
		}
	}
	dtaps[*d.Prod] = true
	for _, i := range d.NonProd {
		if err := ValidateIDPart(cnf, i); err != nil {
			return fmt.Errorf("non prod DTAP id: %w", err)
		}
		if _, ok := dtaps[i]; ok {
			return &FormattingError{fmt.Sprintf("duplicate DTAP: %s", i)}
		}
	}
	if d.isEmpty() {
		return &FormattingError{fmt.Sprintf("empty dtap spec")}
	}
	return nil
}

func (d DTAPSpec) isEmpty() bool {
	return d.Prod == nil && len(d.NonProd) == 0
}

func (d DTAPSpec) HasDTAP(dtap string) bool {
	for _, i := range d.NonProd {
		if dtap == i {
			return true
		}
	}
	if d.Prod != nil {
		return dtap == *d.Prod
	}
	return false
}

func (d DTAPSpec) Count() int {
	if d.Prod != nil {
		return len(d.NonProd) + 1
	}
	return len(d.NonProd)
}
