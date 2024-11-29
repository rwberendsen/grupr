package semantics

import (
	"maps"

	"github.com/rwberendsen/grupr/internal/syntax"
)

type DTAPSpec struct {
	Prod string
	NonProd map[string]bool
	DTAPRendering syntax.Rendering
}

func newDTAPSpec(dsSyn syntax.DTAPSpec, dtapRendering syntax.Rendering) DTAPSpec {
	if dsSyn.IsEmpty() {
		// Not specifying any DTAP info means your objects will be considered as production, and you cannot use the [dtap] expansion
		return DTAPSpec{
			Prod: "",
			DTAPRendering: syntax.Rendering{}, // empty rendering
		 }
	}
	dsSem := DTAPSpec{
		Prod: dsSyn.Prod,
		NonProd: make(map[string]bool, len(dsSyn.NonProd)),
		DTAPRendering: make(syntax.Rendering, len(dsSyn.NonProd) + 1),
	}
	dsSem.DTAPRendering[dsSem.Prod] = dsSem.Prod
	for _, d := range dsSyn.NonProd {
		dsSem.NonProd[d] = true
		dsSem.DTAPRendering[d] = d
	}
	for d, r := range dtapRendering {
		dsSem.DTAPRendering[d] = r
	}
	return dsSem
}

func (lhs DTAPSpec) Equal(rhs DTAPSpec) bool {
	return lhs.Prod == rhs.Prod &&
	       maps.Equal(lhs.NonProd, rhs.NonProd) &&
               maps.Equal(lhs.DTAPRendering, rhs.DTAPRendering)
}
