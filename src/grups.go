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
	objects        []ObjExpr            // parsed object expressions
	objectsExclude []ObjExpr            // parsed object expressions
}

type ProductInterface struct {
	Product        string    `yaml:"product"`
	Interface      string    `yaml:"interface"`
	objects        []ObjExpr // parsed object expressions
	objectsExclude []ObjExpr // parsed object expressions
}

type Interface struct {
	Objects        []string
	ObjectsExclude []string  `yaml:"objects_exclude,omitempty"`
	objects        []ObjExpr // parsed object expressions
	objectsExclude []ObjExpr // parsed object expressions
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
	for k, v := range p.Interfaces {
		if !validId.MatchString(k) {
			return fmt.Errorf("invalid interface id")
		}
		if err := v.validate(); err != nil {
			return err
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
	for _, obj_expr := range p.Objects {
		parsed, err := parse_obj_expr(obj_expr)
		if err != nil {
			return fmt.Errorf("parsing obj expr: %", err)
		}
		p.objects = append(p.objects, parsed)
	}
	for _, obj_expr := range p.ObjectsExclude {
		parsed, err := parse_obj_expr(obj_expr)
		if err != nil {
			return fmt.Errorf("parsing obj expr: %", err)
		}
		p.objectsExclude = append(p.objectsExclude, parsed)
	}
	return nil
}

func (i *Interface) validate() error {
	for _, obj_expr := range i.Objects {
		parsed, err := parse_obj_expr(obj_expr)
		if err != nil {
			return fmt.Errorf("parsing obj expr: %", err)
		}
		i.objects = append(i.objects, parsed)
	}
	for _, obj_expr := range i.ObjectsExclude {
		parsed, err := parse_obj_expr(obj_expr)
		if err != nil {
			return fmt.Errorf("parsing obj expr: %", err)
		}
		i.objectsExclude = append(i.objectsExclude, parsed)
	}
	return nil
}
