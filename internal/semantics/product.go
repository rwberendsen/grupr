package semantics

import (
	"fmt"
	"regexp"

	"github.com/rwberendsen/grupr/internal/syntax"
	"golang.org/x/exp/maps"
)

type Product struct {
	ID	 string
	DTAPs      syntax.DTAPSpec `yaml:"dtaps,flow,omitempty"` // TODO: allow rendering a DTAP as an empty string in an identifier, e.g., omit to DTAP for production data
	Consumes   map[InterfaceID]bool `yaml:",omitempty"`
	InterfaceMetadata // TODO: lift this out into Grupin.Interfaces!?
}

func (lhs Product) disjoint(rhs Product) bool {
	if !lhs.Matcher.disjoint(rhs.Matcher) {
		return false
	}
	for _, l := range lhs.Interfaces {
		if !l.Matcher.disjoint(rhs.Matcher) {
			return false
		}
		for _, r := range rhs.Interfaces {
			if !r.Matcher.disjoint(lhs.Matcher) {
				return false
			}
			if !r.Matcher.disjoint(l.Matcher) {
				return false
			}
		}
	}
	return true
}

func newProduct(p_syn syntax.Product) (Product, error) {
	p_sem := Product{
		ID: p_syn.ID,
		DTAPs:      p_syn.DTAPs,
		Consumes:   map[InterfaceID]bool{},
		InterfaceMetadata: newInterfaceMetadata(pSyn.InterfaceMetadata, p_syn.ID),
	}
	for _, i := range p_syn.Consumes {
		iid := newInterfaceId(i)
		if _, ok := p_sem.Consumes[iid]; ok {
			return p_sem, fmt.Errorf("duplicate consumed interface id")
		}
		p_sem.Consumes[iid] = true
	}
	return p_sem, nil
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
	for k_o := range o.Interfaces {
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
