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
	dtaps    map[string]bool
	matcher  matcher
	consumes map[ProductInterface]bool

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
	matcher matcher

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
	if err := g.allDisjoint(); err != nil {
		return err
	}
	return nil
}

func (g *Grups) allDisjoint() error {
	keys := maps.Keys(g.Products)
	if len(keys) < 2 {
		return nil
	}
	for i := 0; i < len(keys)-1; i++ {
		for j := i + 1; j < len(keys); j++ {
			if !g.Products[keys[i]].disjoint(g.Products[keys[j]]) {
				return fmt.Errorf("overlapping products '%s' and '%s'", keys[i], keys[j])
			}
		}
	}
	return nil
}

func (p *Product) disjoint(o *Product) bool {
	return true
	// TODO implement
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
		if err := v.validate(pkey, k); err != nil {
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
	if m, err := p.matcher.parse(p.Objects, p.ObjectsExclude); err != nil {
		return fmt.Errorf("invalid object matching expressions in product %s: %s", pkey, err)
	} else {
		p.matcher = m
	}
	return nil
}

func (i *Interface) validate(pkey string, ikey string) error {
	if m, err := i.matcher.parse(i.Objects, i.ObjectsExclude); err != nil {
		return fmt.Errorf("invalid object matching expressions in product %s, interface %s: %s", pkey, ikey, err)
	} else {
		i.matcher = m
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
	if equal := p.matcher.equals(o.matcher); !equal {
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
	return i.matcher.equals(j.matcher)
}
