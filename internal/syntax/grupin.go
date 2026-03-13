package syntax

import (
	"fmt"
	"io"
	"strings"

	"gopkg.in/yaml.v3"
)

type Grupin struct {
	Classes           map[string]Class
	GlobalUserGroups  *GlobalUserGroups
	UserGroupMappings map[string]UserGroupMapping
	Products          map[string]Product
	Interfaces        map[InterfaceID]Interface
	ServiceAccounts   map[string]ServiceAccount
}

func NewGrupin(cnf *Config, r io.Reader) (Grupin, error) {
	dec := yaml.NewDecoder(r)
	dec.KnownFields(true)
	g := Grupin{
		UserGroupMappings: map[string]UserGroupMapping{},
		Products:          map[string]Product{},
		Interfaces:        map[InterfaceID]Interface{},
		ServiceAccounts:   map[string]ServiceAccount{},
	}
	for {
		var e ElmntOr
		err := dec.Decode(&e)
		if err == io.EOF {
			break
		}
		if err != nil {
			return g, fmt.Errorf("decoding YAML: %w", err)
		}
		if err := e.validateAndAdd(cnf, &g); err != nil {
			return g, fmt.Errorf("decoding YAML: %w", err)
		}
	}
	if g.Classes == nil {
		return g, fmt.Errorf("no classes found")
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
