package semantics

import (
	"encoding/json/v2"
)

type ActionDTAPs struct {
	Prod    string   `json:",omitempty"`
	NonProd []string `json:"non_prod,omitempty"`
}

type ActionScope struct {
	Product    string
	DTAPS      ActionDTAPs `json:"dtaps,"`
	Interfaces []string    `json:",omitempty"`
}

func (a ActionScope) String() string {
	if b, err := json.Marshal(a); err != nil {
		panic("could not unmarshal ActionScope instance")
	} else {
		return string(b)
	}
}
