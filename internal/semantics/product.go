package semantics

import (
	"fmt"
	"regexp"

	"github.com/rwberendsen/grupr/internal/syntax"
	"golang.org/x/exp/maps"
)

type Product struct {
	ID	 string
	DTAPs      syntax.DTAPSpec `yaml:"dtaps,flow,omitempty"`
	Consumes   map[syntax.InterfaceID]bool `yaml:",omitempty"`
	Matcher	Matcher
	InterfaceMetadata
	Interfaces map[string]Interface
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

func newProduct(pSyn syntax.Product, ug syntax.UserGroups) (Product, error) {
	pSem := Product{
		ID: pSyn.ID,
		DTAPs:      pSyn.DTAPs,
		Consumes:   map[syntax.InterfaceID]bool{},
		Interfaces map[string]Interface{}
	}
	if i, err := newInterfaceMetadata(pSyn.InterfaceMetadata, ug, nil) {
		return pSem, fmt.Errorf("product %s: interface metadata: %v", pSem.ID, err)
	} else {
		pSem.InterfaceMetadata = i
	}
	if m, err := newMatcher(pSyn.Objects, pSyn.ObjectsExclude, pSem.InterfaceMetadata); err != nil {
		return pSem, fmt.Errorf("product %s: matcher: %s", pSem.ID, err)
	} else {
		pSem.Matcher = m
	}
	for _, iid := range pSyn.Consumes {
		if _, ok := pSem.Consumes[iid]; ok {
			return pSem, fmt.Errorf("duplicate consumed interface id")
		}
		pSem.Consumes[iid] = true
	}
	return pSem, nil
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
