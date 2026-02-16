package semantics

import (
	"iter"
	"strings"
	"text/template"

	"github.com/rwberendsen/grupr/internal/syntax"
)

type TmplDataDTAPUG struct {
	DTAP  string
	DTAPs map[string]syntax.Rendering
	UG    string
	UGs   map[string]syntax.Rendering
}

func renderTmplDataDTAPUG(s string, dtaps iter.Seq[string], dtapRenderings map[string]syntax.Rendering,
	userGroups map[string]struct{}, userGroupRenderings map[string]syntax.Rendering) (map[string]map[ObjExprAttr]struct{}, error) {
	r := map[string]map[ObjExprAttr]struct{}{} // K1: rendered template
	for dtap := range dtaps {
		for ug := range userGroups {
			data := TmplDataDTAPUG{DTAP: dtap, DTAPs: dtapRenderings, UG: ug, UGs: userGroupRenderings}
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
			r[b.String()][ObjExprAttr{DTAP: dtap, UserGroup: ug}] = struct{}{}
		}
	}
	return r, nil
}
