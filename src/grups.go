package main

import (
	"fmt"
	"maps"
	"regexp"

	"gopkg.in/yaml.v3"
)

var validId *regexp.Regexp = regexp.MustCompile(`^[a-z_][a-z0-9_]*$`)

type Grups struct {
	// use pointer to Product, cause during validation we enrich the original Product objects
	Products map[string]*Product
}

type Product struct {
	DTAPs          []string `yaml:",flow,omitempty"`
	Objects        []string `yaml:",omitempty"`
	ObjectsExclude []string `yaml:"objects_exclude,omitempty"`
	// use pointer to Interface, cause during validation we enrich Interface
	Interfaces map[string]*Interface `yaml:",omitempty"`
	Consumes   []ProductInterface    `yaml:",omitempty"`

	// lowercased fields are added during validation
	dtaps          map[string]bool
	objects        map[ObjExpr]bool
	objectsExclude map[ObjExpr]bool
	consumes       map[ProductInterface]bool
}

type ProductInterface struct {
	Product   string `yaml:"product"`
	Interface string `yaml:"interface"`
}

type Interface struct {
	Objects        []string
	ObjectsExclude []string `yaml:"objects_exclude,omitempty"`

	// lowercased fields are added during validation
	objects        map[ObjExpr]bool
	objectsExclude map[ObjExpr]bool
}

type GrupsDiff struct {
	Created map[string]*Product
	Deleted map[string]*Product
	Updated map[string]ProductDiff
}

type ProductDiff struct {
	Old *Product
	New *Product
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

func (grupsDiff *GrupsDiff) String() string {
	data, err := yaml.Marshal(grupsDiff)
	if err != nil {
		panic("grupsDiff could not be marshalled")
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
	p.dtaps = make(map[string]bool)
	for _, i := range p.DTAPs {
		if !validId.MatchString(i) {
			return fmt.Errorf("invalid dtap")
		}
		p.dtaps[i] = true
	}
	for k, v := range p.Interfaces {
		if !validId.MatchString(k) {
			return fmt.Errorf("invalid interface id")
		}
		if err := v.validate(); err != nil {
			return err
		}
	}
	p.consumes = make(map[ProductInterface]bool)
	for _, i := range p.Consumes {
		if i.Product == pkey {
			return fmt.Errorf("consuming interface from own product")
		}
		if q, ok := g.Products[i.Product]; !ok {
			return fmt.Errorf("consumed product not found")
		} else if _, ok := q.Interfaces[i.Interface]; !ok {
			return fmt.Errorf("consumed interface not found")
		}
		p.consumes[i] = true
	}
	p.objects = make(map[ObjExpr]bool)
	for _, obj_expr := range p.Objects {
		parsed, err := parse_obj_expr(obj_expr)
		if err != nil {
			return fmt.Errorf("parsing obj expr: %", err)
		}
		p.objects[parsed] = true
	}
	p.objectsExclude = make(map[ObjExpr]bool)
	for _, obj_expr := range p.ObjectsExclude {
		parsed, err := parse_obj_expr(obj_expr)
		if err != nil {
			return fmt.Errorf("parsing obj expr: %", err)
		}
		p.objectsExclude[parsed] = true
	}
	return nil
}

func (i *Interface) validate() error {
	i.objects = make(map[ObjExpr]bool)
	for _, obj_expr := range i.Objects {
		parsed, err := parse_obj_expr(obj_expr)
		if err != nil {
			return fmt.Errorf("parsing obj expr: %", err)
		}
		i.objects[parsed] = true
	}
	i.objectsExclude = make(map[ObjExpr]bool)
	for _, obj_expr := range i.ObjectsExclude {
		parsed, err := parse_obj_expr(obj_expr)
		if err != nil {
			return fmt.Errorf("parsing obj expr: %", err)
		}
		i.objectsExclude[parsed] = true
	}
	return nil
}

func getGrupsDiff(old *Grups, new *Grups) *GrupsDiff {
	if old == nil {
		return &GrupsDiff{new.Products, nil, nil}
	}
	diff := GrupsDiff{make(map[string]*Product), make(map[string]*Product), make(map[string]ProductDiff)}
	for k_old, v_old := range old.Products {
		v_new, ok := new.Products[k_old]
		if !ok {
			diff.Deleted[k_old] = v_old
		} else if equal := v_old.equals(v_new); !equal {
			diff.Updated[k_old] = ProductDiff{v_old, v_new}
		}
	}
	for k_new, v_new := range new.Products {
		_, ok := old.Products[k_new]
		if !ok {
			diff.Created[k_new] = v_new
		}
	}
	return &diff
}

func (p *Product) equals(o *Product) bool {
	if equal := maps.Equal(p.dtaps, o.dtaps); !equal {
		return false
	}
	if equal := maps.Equal(p.objects, o.objects); !equal {
		return false
	}
	if equal := maps.Equal(p.objectsExclude, o.objectsExclude); !equal {
		return false
	}
	// interfaces
	for k_p, v_p := range p.Interfaces {
		v_o, ok := o.Interfaces[k_p]
		if !ok {
			return false
		}
		if equal := v_p.equals(v_o); !equal {
			return false
		}
	}
	for k_o, _ := range o.Interfaces {
		_, ok := p.Interfaces[k_o]
		if !ok {
			return false
		}
	}
	// consumes
	if equal := maps.Equal(p.consumes, o.consumes); !equal {
		return false
	}
	return true
}

func (i *Interface) equals(j *Interface) bool {
	if equal := maps.Equal(i.objects, j.objects); !equal {
		return false
	}
	if equal := maps.Equal(i.objectsExclude, j.objectsExclude); !equal {
		return false
	}
	return true
}
