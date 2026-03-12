package syntax

import (
	"fmt"
)

type DTAPSpec struct {
	NonProd []string `yaml:"non_prod,flow,omitempty"`
	Prod    *string  `yaml:",omitempty"`
}

func (d DTAPSpec) validateNormalize(cnf *Config) (DTAPSpec, error) {
	// If there are values, they need to be valid ID parts, and unique.
	// A completely empty dtap spec is also okay, but will be replaced with a default
	// DTAPSpec consisting of a single production DTAP called cnf.DefaultProdDTAPName
	if d.Prod == nil && len(d.NonProd) == 0 {
		d.Prod = &cnf.DefaultProdDTAPName
	}
	dtaps := map[string]bool{}
	if d.Prod != nil {
		if err := ValidateIDPart(cnf, *d.Prod); err != nil {
			return d, fmt.Errorf("prod DTAP id: %w", err)
		}
		dtaps[*d.Prod] = true
	}
	for _, i := range d.NonProd {
		if err := ValidateIDPart(cnf, i); err != nil {
			return d, fmt.Errorf("non prod DTAP id: %w", err)
		}
		if _, ok := dtaps[i]; ok {
			return d, &FormattingError{fmt.Sprintf("duplicate DTAP: %s", i)}
		}
	}
	return d, nil
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

func (d DTAPSpec) HasProd() bool {
	return d.Prod != nil
}

func (d DTAPSpec) IsProd(dtap string) bool {
	return d.Prod != nil && dtap == *d.Prod
}

func (d DTAPSpec) Count() int {
	if d.Prod != nil {
		return len(d.NonProd) + 1
	}
	return len(d.NonProd)
}
