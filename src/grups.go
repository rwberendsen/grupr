package main

import (
	"fmt"
	"regexp"

	"gopkg.in/yaml.v3"
)

var validId *regexp.Regexp = regexp.MustCompile(`^[a-z_][a-z0-9_]*$`)

type Grups struct {
	Products map[string]Product
}

type Product struct {
	DTAPs          []string             `yaml:",flow,omitempty"`
	Objects        []string             `yaml:",omitempty"`
	ObjectsExclude []string             `yaml:"objects_exclude,omitempty"`
	Interfaces     map[string]Interface `yaml:",omitempty"`
	Consumes       []ProductInterface   `yaml:",omitempty"`
}

type ProductInterface struct {
	Product   string `yaml:"product"`
	Interface string `yaml:"interface"`
}

type Interface struct {
	Objects        []string
	ObjectsExclude []string `yaml:"objects_exclude,omitempty"`
}

func getGrups(data []byte) (*Grups, error) {
	grups := Grups{}
	err := yaml.Unmarshal(data, &grups)
	if err != nil {
		err = fmt.Errorf("unmarshalling groups: %s", err)
		return nil, err
	}
	return &grups, nil
}

func (grups *Grups) String() string {
	data, err := yaml.Marshal(grups)
	if err != nil {
		panic("grups could not be marshalled")
	}
	return string(data)
}

func (g *Grups) validate() error {
	for k, v := range g.Products {
		if !validId.MatchString(k) {
			return fmt.Errorf("invalid product id")
		}
		if err := v.validate(g, k); err != nil {
			return err
		}
	}
	return nil
}

func (p *Product) validate(g *Grups, pkey string) error {
	for k, _ := range p.Interfaces {
		if !validId.MatchString(k) {
			return fmt.Errorf("invalid interface id")
		}
	}
	for _, i := range p.Consumes {
		if i.Product == pkey {
			return fmt.Errorf("consuming interface from own product")
		}
		if q, ok := g.Products[i.Product]; !ok {
			return fmt.Errorf("consumed product not found")
		} else if _, ok := q.Interfaces[i.Interface]; !ok {
			return fmt.Errorf("consumed interface not found")
		}
	}
	return nil
}
