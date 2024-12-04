package syntax

import (
	"io"
	"fmt"
	"strings"

	"gopkg.in/yaml.v3"
)

type Grupin struct {
	Classes			map[string]Class
	AllowedUserGroups	map[string]bool
	Products	map[string]Product
	Interfaces	map[InterfaceID]Interface
}

func NewGrupin(r io.Reader) (Grupin, error) {
	dec := yaml.NewDecoder(r)
	dec.KnownFields(true)
	g := Grupin{
		Products: map[string]Product{},
		Interfaces: map[InterfaceID]Interface{},
	}
	for {
		var e ElmntOr;
		err := dec.Decode(&e)
		if err == io.EOF {
			break
		}
		if err != nil {
			return g, fmt.Errorf("decoding YAML: %w", err)
		}
		if err := e.validateAndAdd(&g); err != nil {
			return g, fmt.Errorf("decoding YAML: %w", err)
		}
	}
	return g, nil
}

func (g *Grupin) String() string {
	var w *strings.Builder
	enc := yaml.NewEncoder(w)
	for e := range g.Products {
		err := enc.Encode(e)
		if err != nil {
			panic("grupin could not be encoded")
		}
	}
	return w.String()
}
