package syntax

import (
	"fmt"
)

// This generic function is in the syntax package because we only need it
// because sets are awkward to define in YAML, which is why we allow them to be
// specified as lists. Therefore, we need to check often in this specific
// package whether the elements in a list are actually unique
func hasUniqueStrings(l []string) error {
	// TODO: consider using generics for simple util functions like this
	m := map[string]bool{}
	for _, i := range l {
		if _, ok := m[i]; ok {
			return fmt.Errorf("duplicate: '%s'", i)
		}
		m[i] = true
	}
	return nil
}
