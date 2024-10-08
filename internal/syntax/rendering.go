package syntax

import (
	"fmt"
)

var validRendering *regexp.Regexp = regexp.MustCompile(`^[A-Za-z0-9_]+$`)

type Rendering map[string]string

func (r Rendering) validate() error {
	for k, v := r {
		if err := validateID(k); err != nil { return err }
		if !validRendering.MatchString(v) { return FormattingError{fmt.Sprintf("key '%s': invalid rendering '%s'", k, v)} }
	}
	return nil
}
