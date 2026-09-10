package semantics

import (
	"encoding/json"
	"iter"
)

type ActionDTAPs struct {
	Prod    string          `json:"prod,omitempty"`
	NonProd map[string]bool `json:"non_prod,omitempty"`
}

type ActionScope struct {
	Product    string          `json:"product,"`
	DTAPs      ActionDTAPs     `json:"dtaps,"`
	Interfaces map[string]bool `json:"interfaces,omitempty"`
}

func NewActionScope(product string) ActionScope {
	return ActionScope{
		Product: product,
		DTAPs:   ActionDTAPs{},
	}
}

func (a ActionScope) String() string {
	if b, err := json.Marshal(a); err != nil {
		panic("could not unmarshal ActionScope instance")
	} else {
		return string(b)
	}
}

func (a ActionScope) AddDTAP(dtap string, isProd bool) ActionScope {
	if isProd {
		a.DTAPs.Prod = dtap
		return a
	}
	if a.DTAPs.NonProd == nil {
		a.DTAPs.NonProd = map[string]bool{}
	}
	a.DTAPs.NonProd[dtap] = true
	return a
}

func (a ActionScope) AddInterface(i string) ActionScope {
	if a.Interfaces == nil {
		a.Interfaces = map[string]bool{}
	}
	a.Interfaces[i] = true
	return a
}

func (a ActionScope) AllDTAPsProdFirst() iter.Seq[string] {
	return func(yield func(string) bool) {
		if a.DTAPs.Prod != "" {
			if !yield(a.DTAPs.Prod) {
				return
			}
		}
		for dtap := range a.DTAPs.NonProd {
			if !yield(dtap) {
				return
			}
		}
	}
}
