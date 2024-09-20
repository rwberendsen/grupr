package syntax

import (
	"fmt"
)

type DTAPSpec struct {
	NonProd		[]ID		`yaml:"non_prod,flow,omitempty`
	Prod		ID `yaml:",omitempty"`
}

func (d DTAPSpec) validate() error {
	dtaps := map[ID]bool{}
	if err := d.Prod.validate(); err != nil { return fmt.Errorf("prod DTAP id: %s", err) }
	dtaps[d.Prod] = true
	for i := range d.NonProd {
		if err := i.validate(); err != nil { return fmt.Errorf("non prod DTAP id: %s", err) }
		if _, ok := dtaps[i]; ok {
			return fmt.Errorf("duplicate DTAP: %s", i)
		}
	}
	return nil
}
