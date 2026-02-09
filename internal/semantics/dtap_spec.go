package semantics

import (
	"iter"
	"maps"

	"github.com/rwberendsen/grupr/internal/syntax"
	"github.com/rwberendsen/grupr/internal/util"
)

type DTAPSpec struct {
	Prod          *string
	NonProd       map[string]struct{}
	DTAPRenderings map[string]syntax.Rendering
}

func newDTAPSpec(cnf *Config, dsSyn *syntax.DTAPSpec, dtapRenderings map[string]syntax.Rendering) (DTAPSpec, error) {
	var dsSem DTAPSpec
	if dsSyn == nil {
		// Not specifying any DTAP info means you will get a default DTAP spec, which has only a production DTAP
		dsSem.Prod = &cnf.DefaultProdDTAPName,
	} else {
		dsSem.Prod = dsSyn.Prod
		if dsSyn.Prod != nil {
			dsSem.Prod = dsSyn.Prod 
		}
		dsSem.NonProd = make(map[string]struct{}, len(dsSyn.NonProd))
		for _, d := range dsSyn.NonProd {
			dsSem.NonProd[d] = struct{}{}
		}
	}
	dsSem.DTAPRenderings = make(map[string]syntax.Rendering, len(dtapRenderings))
	for k, r := range dtapRenderings {
		dsSem.DTAPRenderings[k] = syntax.Rendering{}
		for dtap := range dsSem.All() {
			dsSem.DTAPRenderings[k][dtap] = dtap // default value	
		}
		for dtap, v := range r {
			if !dsSem.HasDTAP(dtap) {
				return dsSem, &SetLogicError{fmt.Sprintf("dtap_rendering '%s': unknown dtap '%s'", k, dtap)}
			}
			dsSem.DTAPRenderings[k][dtap] = v
		}
	}
	return dsSem, nil
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
	return util.EqualStrPtr(lhs.Prod, rhs.Prod) && 
	       maps.Equal(lhs.NonProd, rhs.NonProd) &&
	       maps.EqualFunc(lhs.DTAPRenderings, rhs.DTAPRenderings, syntax.Rendering.Equal)
}
