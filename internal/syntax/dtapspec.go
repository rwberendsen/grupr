package syntax

import (
	"fmt"
)

type DTAPSpec struct {
	NonProd		[]string		`yaml:"non_prod,flow,omitempty`
	Prod		string `yaml:",omitempty"` // "" means no prod DTAP exists
}

func (d DTAPSpec) validate() error {
	dtaps := map[string]bool{}
	if d.Prod != "" {
		if err := validateID(d.Prod); err != nil { return fmt.Errorf("prod DTAP id: %w", err) }
	}
	dtaps[d.Prod] = true
	for i := range d.NonProd {
		if err := validateID(i); err != nil { return fmt.Errorf("non prod DTAP id: %w", err) }
		if _, ok := dtaps[i]; ok {
			return FormattingError{fmt.Sprintf("duplicate DTAP: %s", i)}
		}
	}
	return nil
}

func (d DTAPSpec) IsEmpty() bool {
	return d.Prod == "" && len(d.NonProd) == 0
}

func (d DTAPSpec) HasDTAP(dtap string) {
	if dtap == "" { return false } // "" is a zero value we interpret as a non-existent (not specified) DTAP
	for _, i := range d.NonProd {
		if dtap == i { return true }
	}
	return dtap == d.Prod
}

func (d DTAPSpec) Count() int {
	return len(d.NonProd) + 1
}
