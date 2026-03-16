package syntax

import (
	"fmt"
	"maps"
)

type Rendering map[string]string

func (r Rendering) validate() error {
	values := make(map[string]struct{}, len(r))
	for k, v := range r {
		if _, ok := values[v]; ok {
			return &FormattingError{fmt.Sprintf("key '%s': renders to same value '%s' as another key", k, v)}
		}
		if _, ok := r[v]; ok && k != v {
			return &FormattingError{fmt.Sprintf("key '%s': renders to value '%s' which equals another key", k, v)}
		}
		values[v] = struct{}{}
	}
	return nil
}

func (lhs Rendering) Equal(rhs Rendering) bool {
	return maps.Equal(lhs, rhs)
}
