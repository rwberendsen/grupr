package syntax

import (
	"fmt"
)

type DTAPSpec struct {
	NonProd		[]ID		`yaml:"non_prod,flow,omitempty`
	Prod		ID `yaml:",omitempty"`
}

func (d DTAPSpec) validate() error {
	for i := range d.NonProd {
		if err := i.validate(); err != nil { return fmt.Errorf("non prod DTAP id: %s", err) }
	}
	return nil
}
