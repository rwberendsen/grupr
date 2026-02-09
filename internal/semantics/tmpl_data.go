package semantics

import (
	"github.com/rwberendsen/grupr/internal/syntax"
)

type TmplData struct {
	DTAP	string
	DTAPs	map[string]syntax.Rendering
}

type TmplDataUG struct {
	DTAP	string
	DTAPs	map[string]syntax.Rendering
	UG	string
	UGs	map[string]syntax.Rendering
}
