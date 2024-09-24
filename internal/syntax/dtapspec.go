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
		if err := validateID(d.Prod); err != nil { return fmt.Errorf("prod DTAP id: %s", err) }
	}
	dtaps[d.Prod] = true
	for i := range d.NonProd {
		if err := validateID(i); err != nil { return fmt.Errorf("non prod DTAP id: %s", err) }
		if _, ok := dtaps[i]; ok {
			return fmt.Errorf("duplicate DTAP: %s", i)
		}
	}
	return nil
}

func (d DTAPSpec) isEmpty() bool {
	return d.Prod == "" && len(d.NonProd) == 0
}
