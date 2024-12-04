package semantics

import (
	"fmt"

	"github.com/rwberendsen/grupr/internal/syntax"
)

type Classification int

func newClassification(classification string, classes map[string]syntax.Class) (Classification, error) {
	if c, ok := classes[classification]; ok {
		return Classification(c.Level), nil
	}
	return 0, &SetLogicError{fmt.Sprintf("Unknown classification: '%s'", classification)}
}
