package semantics

import (
	"iter"
	"strings"
	"text/template"

	"github.com/rwberendsen/grupr/internal/syntax"
)

type TmplDataDTAP struct {
	DTAP  string
	DTAPs map[string]syntax.Rendering
}

func renderTmplDataDTAP(s string, dtaps iter.Seq[string], dtapRenderings map[string]syntax.Rendering) (map[string]map[ObjExprAttr]struct{}, error) {
	r := map[string]map[ObjExprAttr]struct{}{} // K1: rendered template
	for dtap := range dtaps {
		data := TmplDataDTAP{DTAP: dtap, DTAPs: dtapRenderings}
		tmpl, err := template.New("t").Parse(s)
		if err != nil {
			return r, err
		}
		var b strings.Builder
		if err = tmpl.Execute(&b, data); err != nil {
			return r, err
		}
		if _, ok := r[b.String()]; !ok {
			r[b.String()] = map[ObjExprAttr]struct{}{}
		}
		r[b.String()][ObjExprAttr{DTAP: dtap}] = struct{}{}
	}
	return r, nil
}
