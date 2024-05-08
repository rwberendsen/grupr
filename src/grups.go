package main

import (
	"fmt"
	"regexp"

	"golang.org/x/exp/maps"
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

	// fields added by validation
	dtaps        map[string]bool
	exprs        map[expr]bool
	exprsExclude map[expr]bool
	consumes     map[ProductInterface]bool

	// fields added by querying Snowflake
	matchedInclude accountObjs
	matchedExclude accountObjs
	matched        accountObjs
}

type ProductInterface struct {
	Product   string `yaml:"product"`
	Interface string `yaml:"interface"`
}

type Interface struct {
	Objects        []string
	ObjectsExclude []string `yaml:"objects_exclude,omitempty"`

	// lowercased fields are added during validation
	exprs        map[expr]bool
	exprsExclude map[expr]bool

	// fields added by querying Snowflake
	matchedInclude accountObjs
	matchedExclude accountObjs
	matched        accountObjs
}

type grupsDiff struct {
	created map[string]*Product
	deleted map[string]*Product
	updated map[string]productDiff
}

type productDiff struct {
	old *Product
	new *Product
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

func (grupsDiff *grupsDiff) String() string {
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
	// TODO validate consistency accross products, e.g., each object is allowed
	// to be matched by one expression only, accross products;
	// although within a product, an object may be matched both as part of the
	// project itself and as part of one or more interfaces
	return nil
}

func (p *Product) validate(g *Grups, pkey string) error {
	p.dtaps = map[string]bool{}
	for _, i := range p.DTAPs {
		if !validId.MatchString(i) {
			return fmt.Errorf("invalid dtap")
		}
		if _, ok := p.dtaps[i]; ok {
			return fmt.Errorf("duplicate dtap")
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
	p.consumes = map[ProductInterface]bool{}
	for _, i := range p.Consumes {
		if i.Product == pkey {
			return fmt.Errorf("consuming interface from own product")
		}
		if q, ok := g.Products[i.Product]; !ok {
			return fmt.Errorf("consumed product not found")
		} else if _, ok := q.Interfaces[i.Interface]; !ok {
			return fmt.Errorf("consumed interface not found")
		}
		if _, ok := p.consumes[i]; ok {
			return fmt.Errorf("duplicate consumed interface id")
		}
		p.consumes[i] = true
	}
	p.exprs = map[expr]bool{}
	for _, objExpr := range p.Objects {
		parsed, err := parseObjExpr(objExpr)
		if err != nil {
			return fmt.Errorf("parsing obj expr: %", err)
		}
		if _, ok := p.exprs[parsed]; ok {
			return fmt.Errorf("duplicate expr")
		}
		p.exprs[parsed] = true
	}
	if ok := allDisjoint(p.exprs); !ok {
		return fmt.Errorf("non disjoint set of exprs")
	}
	p.exprsExclude = map[expr]bool{}
	for _, objExpr := range p.ObjectsExclude {
		parsed, err := parseObjExpr(objExpr)
		if err != nil {
			return fmt.Errorf("parsing obj expr: %", err)
		}
		if _, ok := p.exprsExclude[parsed]; ok {
			return fmt.Errorf("duplicate expr")
		}
		p.exprsExclude[parsed] = true
	}
	if ok := allDisjoint(p.exprsExclude); !ok {
		return fmt.Errorf("non disjoint set of exprs")
	}
	return nil
}

func (i *Interface) validate() error {
	i.exprs = map[expr]bool{}
	for _, objExpr := range i.Objects {
		parsed, err := parseObjExpr(objExpr)
		if err != nil {
			return fmt.Errorf("parsing obj expr: %", err)
		}
		if _, ok := i.exprs[parsed]; ok {
			return fmt.Errorf("duplicate expr")
		}
		i.exprs[parsed] = true
	}
	if ok := allDisjoint(i.exprs); !ok {
		return fmt.Errorf("non disjoint set of exprs")
	}
	i.exprsExclude = map[expr]bool{}
	for _, objExpr := range i.ObjectsExclude {
		parsed, err := parseObjExpr(objExpr)
		if err != nil {
			return fmt.Errorf("parsing obj expr: %", err)
		}
		if _, ok := i.exprsExclude[parsed]; ok {
			return fmt.Errorf("duplicate expr")
		}
		i.exprsExclude[parsed] = true
	}
	if ok := allDisjoint(i.exprsExclude); !ok {
		return fmt.Errorf("non disjoint set of exprs")
	}
	return nil
}

func getGrupsDiff(old *Grups, new *Grups) *grupsDiff {
	if old == nil {
		return &grupsDiff{new.Products, nil, nil}
	}
	diff := grupsDiff{map[string]*Product{}, map[string]*Product{}, map[string]productDiff{}}
	for k_old, v_old := range old.Products {
		v_new, ok := new.Products[k_old]
		if !ok {
			diff.deleted[k_old] = v_old
		} else if equal := v_old.equals(v_new); !equal {
			diff.updated[k_old] = productDiff{v_old, v_new}
		}
	}
	for k_new, v_new := range new.Products {
		_, ok := old.Products[k_new]
		if !ok {
			diff.created[k_new] = v_new
		}
	}
	return &diff
}

func (p *Product) equals(o *Product) bool {
	if equal := maps.Equal(p.dtaps, o.dtaps); !equal {
		return false
	}
	if equal := maps.Equal(p.exprs, o.exprs); !equal {
		return false
	}
	if equal := maps.Equal(p.exprsExclude, o.exprsExclude); !equal {
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
	if equal := maps.Equal(i.exprs, j.exprs); !equal {
		return false
	}
	if equal := maps.Equal(i.exprsExclude, j.exprsExclude); !equal {
		return false
	}
	return true
}
