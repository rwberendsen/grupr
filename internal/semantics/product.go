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
	InterfaceMetadata
	Interfaces map[string]Interface
}

func newProduct(pSyn syntax.Product, ug syntax.UserGroups) (Product, error) {
	pSem := Product{
		ID: pSyn.ID,
		DTAPs:      pSyn.DTAPs,
		Consumes:   map[syntax.InterfaceID]bool{},
		Interfaces map[string]Interface{}
	}
	if i, err := newInterfaceMetadata(pSyn.InterfaceMetadata, ug, pSem.DTAPs, nil) {
		return pSem, fmt.Errorf("product id %s: interface metadata: %w", pSem.ID, err)
	} else {
		pSem.InterfaceMetadata = i
	}
	if m, err := newObjectMatcher(pSyn.Objects, pSyn.ObjectsExclude, pSem.InterfaceMetadata); err != nil {
		return pSem, fmt.Errorf("product %s: matcher: %s", pSem.ID, err)
	} else {
		pSem.ObjectMatcher = m
	}
	for _, iid := range pSyn.Consumes {
		if iid.ProducingService == "" && iid.Product == pSem.ID {
			return PolicyError{fmt.Sprintf("product '%s' not allowed to consume own interface '%s'", iid.Product, iid.Interface)}
		}
		if _, ok := pSem.Consumes[iid]; ok {
			return pSem, fmt.Errorf("duplicate consumed interface id")
		}
		pSem.Consumes[iid] = true
	}
	return pSem, nil
}

func (lhs Product) disjoint(rhs Product) bool {
	if !lhs.ObjectMatcher.disjoint(rhs.ObjectMatcher) {
		return false
	}
	for _, l := range lhs.Interfaces {
		if !l.ObjectMatcher.disjoint(rhs.ObjectMatcher) {
			return false
		}
		for _, r := range rhs.Interfaces {
			if !r.ObjectMatcher.disjoint(lhs.ObjectMatcher) {
				return false
			}
			if !r.ObjectMatcher.disjoint(l.ObjectMatcher) {
				return false
			}
		}
	}
	return true
}

func (p Product) equals(o Product) bool {
	if equal := maps.Equal(p.DTAPs, o.DTAPs); !equal {
		return false
	}
	if equal := p.ObjectMatcher.equals(o.ObjectMatcher); !equal {
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
