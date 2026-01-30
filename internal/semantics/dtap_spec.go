package semantics

import (
	"iter"
	"maps"

	"github.com/rwberendsen/grupr/internal/syntax"
)

type DTAPSpec struct {
	Prod          *string
	NonProd       map[string]struct{}
	DTAPRendering syntax.Rendering
}

func newDTAPSpec(cnf *Config, dsSyn *syntax.DTAPSpec, dtapRendering syntax.Rendering) DTAPSpec {
	if dSyn == nil () {
		// Not specifying any DTAP info means you will get a default DTAP spec, which has only a production DTAP
		return DTAPSpec{
			Prod:          &cnf.DefaultProdDTAPName,
		}
	}
	dsSem := DTAPSpec{
		Prod:          dsSyn.Prod,
		NonProd:       make(map[string]struct{}, len(dsSyn.NonProd)),
		DTAPRendering: make(syntax.Rendering, len(dsSyn.NonProd)+1),
	}
	if dsSyn.Prod != nil {
		s := *dsSyn.Prod
		dsSem.Prod = &s // s will escape, but, if we had assigned dsSyn.Prod directly, then dsSyn would not be garbage collected.
		dsSem.DTAPRendering[s] = s // default value when not in dtapRendering
	}
	for _, d := range dsSyn.NonProd {
		dsSem.NonProd[d] = struct{}{}
		dsSem.DTAPRendering[d] = d // default value when not in dtapRendering
	}
	for d, r := range dtapRendering {
		dsSem.DTAPRendering[d] = r // overwrite default value
	}
	return dsSem
}

func (spec DTAPSpec) HasDTAP(dtap string) bool {
	if spec.Prod != nil {
		if *spec.Prod == dtap {
			return true
		}
	}
	_, ok := spec.NonProd[dtap]
	return ok
}

func (spec DTAPSpec) IsProd(dtap string) bool {
	return spec.Prod != nil && dtap == *spec.Prod
}

func (spec DTAPSpec) All() iter.Seq2[string, bool] {
	return func(yield func(string, bool) bool) {
		if spec.Prod != nil {
			if !yield(*spec.Prod, true) {
				return
			}
		}
		for k := range spec.NonProd {
			if !yield(k, false) {
				return
			}
		}
	}
}

func (lhs DTAPSpec) Equal(rhs DTAPSpec) bool {
	if lhs.Prod == nil && rhs.Prod != nil { return false }
	if lhs.Prod != nil && rhs.Prod == nil { return false }
	if lhs.Prod != nil && rhs.Prod != nil && *lhs.Prod != *rhs.Prod { return false }
	return maps.Equal(lhs.NonProd, rhs.NonProd) &&
		maps.Equal(lhs.DTAPRendering, rhs.DTAPRendering)
}
