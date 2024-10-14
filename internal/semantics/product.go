package semantics

import (
	"fmt"

	"github.com/rwberendsen/grupr/internal/syntax"
	"golang.org/x/exp/maps"
)

type Product struct {
	ID	 string `yaml:"id"`
	DTAPs      DTAPSpec `yaml:"dtaps,flow,omitempty"`
	Consumes   map[syntax.InterfaceID]bool `yaml:",omitempty"`
	InterfaceMetadata
	Interfaces map[string]InterfaceMetadata
}

func newProduct(pSyn syntax.Product, allowedUserGroups map[string]bool) (Product, error) {
	pSem := Product{
		ID: pSyn.ID,
		DTAPs:      pSyn.DTAPs,
		Consumes:   map[syntax.InterfaceID]bool{},
		Interfaces map[string]InterfaceMetadata{},
	}
	pSem.DTAPs = newDTAPSpec(pSyn.DTAPs, pSyn.DTAPRendering)
	if pSem.InterfaceMetadata, err := newInterfaceMetadata(pSyn.InterfaceMetadata, allowedUserGroups, pSem.DTAPs.DTAPRendering, nil); err != nil {
		return pSem, fmt.Errorf("product id %s: interface metadata: %w", pSem.ID, err)
	}
	for _, iid := range pSyn.Consumes {
		if iid.ProducingService == "" && iid.ProductID == pSem.ID {
			return PolicyError{fmt.Sprintf("product '%s' not allowed to consume own interface '%s'", iid.ProductID, iid.ID)}
		}
		if _, ok := pSem.Consumes[iid]; ok {
			return pSem, fmt.Errorf("duplicate consumed interface id")
		}
		pSem.Consumes[iid] = true
	}
	return pSem, nil
}

func (lhs Product) disjoint(rhs Product) bool {
	return lhs.ObjectMatcher.disjoint(rhs.ObjectMatcher)
}

func (p Product) equals(o Product) bool {
	// TODO: revisit after recent changes
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
