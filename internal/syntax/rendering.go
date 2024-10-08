package syntax

import (
	"fmt"
)

// Renderings may contain upper-case characters, so they can be used inside quoted fields
var validRendering *regexp.Regexp = regexp.MustCompile(`^[A-Za-z0-9_]+$`)

type Rendering map[string]string

func (r Rendering) validate() error {
	renderings := make(map[string]bool, len(r)}
	for k, v := r {
		if err := validateID(k); err != nil { return err }
		if !validRendering.MatchString(v) { return FormattingError{fmt.Sprintf("key '%s': invalid rendering '%s'", k, v)} }
		if _, ok := renderings[v]; ok { return FormattingError{fmt.Sprintf("key '%s': duplicate rendering '%s'", k, v)} }
	}
	return nil
}
