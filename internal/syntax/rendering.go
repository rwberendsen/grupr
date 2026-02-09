package syntax

import (
	"fmt"
	"maps"
	"regexp"
)

type Rendering map[string]string

func (r Rendering) validate() error {
	inverse := make(map[string]struct{}, len(r))
	for k, v := range r {
		if err := ValidateID(k); err != nil {
			return err
		}
		if _, ok := inverse[v]; ok {
			return &FormattingError{fmt.Sprintf("key '%s': duplicate rendering '%s'", k, v)}
		}
		if _, ok := r[v]; ok && k != v {
			return &FormattingError{fmt.Sprintf("key '%s': rendering '%s' equals another key", k, v)}
		}
		inverse[v] = true
	}
	return nil
}

func (lhs Rendering) Equal(rhs Rendering) bool {
	return maps.Equal(lhs, rhs)
}
