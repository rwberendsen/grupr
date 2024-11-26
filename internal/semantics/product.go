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
			return &PolicyError{fmt.Sprintf("product '%s' not allowed to consume own interface '%s'", iid.ProductID, iid.ID)}
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

func (lhs Product) Equal(rhs Product) bool {
	if lhs.ID != rhs.ID { return false }
	if !lhs.DTAPs.Equal(rhs.DTAPs) { return false }
	if !maps.Equal(lhs.Consumes, rhs.Consumes) { return false }
	if !lhs.InterfaceMetadata.Equal(rhs.InterfaceMetadata) { return false }
	if !maps.EqualFunc(lhs.Interfaces, rhs.Interfaces, InterfaceMetadata.Equal) { return false }
	return true
}
