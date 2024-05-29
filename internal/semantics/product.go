package semantics

import (
	"fmt"

	"golang.org/x/exp/maps"
	"github.com/rwberendsen/grupr/internal/syntax"
)

type Product struct {
	DTAPs    map[string]bool
	Matcher  matcher
	Interfaces map[string]Interface
	Consumes map[syntax.ProductInterface]bool
}

func (p Product) disjoint(o Product) bool {
	return true
	// TODO implement
}

func newProduct(p syntax.Product) (Product, error) {
	r := Product{
		DTAPs: map[string]bool{},
		Interfaces: map[string]Interface{},
		Consumes: map[syntax.ProductInterface]bool{}
	}
	for _, i := range p.DTAPs {
		if !validId.MatchString(i) {
			return fmt.Errorf("invalid dtap")
		}
		if _, ok := r.DTAPs[i]; ok {
			return fmt.Errorf("duplicate dtap")
		}
		r.DTAPs[i] = true
	}
	if m, err := newMatcher(p.Objects, p.ObjectsExclude); err != nil {
		return fmt.Errorf("invalid object matching expressions: %s", err)
	} else {
		r.Matcher = m
	}
	for k, v := range p.Interfaces {
		if !validId.MatchString(k) {
			return fmt.Errorf("invalid interface id: '%s'", k)
		}
		if i, err := newInterface(v); err != nil {
			return fmt.Errorf("invalid interface '%s': %s", k, err)
		} else {
			r.Interfaces[k] = i
		}
	}
	for _, i := range p.Consumes {
		if _, ok := r.Consumes[i]; ok {
			return fmt.Errorf("duplicate consumed interface id")
		}
		r.Consumes[i] = true
	}
	return nil
}

func (p Product) equals(o Product) bool {
	if equal := maps.Equal(p.DTAPs, o.DTAPs); !equal {
		return false
	}
	if equal := p.Matcher.equals(o.Matcher); !equal {
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
	if equal := maps.Equal(p.Consumes, o.Consumes); !equal {
		return false
	}
	return true
}
