package syntax

import (
	"io"
	"fmt"
	"strings"

	"gopkg.in/yaml.v3"
)

type Grupin struct {
	ProducingServices map[string]ProducingService
	Products	map[string]Product
	Interfaces	map[InterfaceID]Interface
}

func NewGrupin(r io.Reader) (*Grupin, error) {
	dec := yaml.NewDecoder(f)
	dec.KnownFields(true)
	var g Grupin
	for {
		var e ElmntOr;
		err := dec.Decode(&e)
		if err == io.EOF {
			break
		}
		if err != nil {
			return g, fmt.Errrorf("decoding YAML: %s", err)
		}
		if err := e.validateAndAdd(&g); err != nil {
			return g, fmt.Errorf("decoding YAML: %s", err)
		}
	}
	return &g, nil
}

func (g *Grupin) String() string {
	w strings.Builder()
	enc := yaml.NewEncoder(w)
	for e := range g {
		err := enc.Encode(e)
		if err != nil {
			panic("gruping could not be encoded")
		}
	}
	return w.String()
}
