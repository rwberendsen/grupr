package syntax

import (
    "fmt"
    "regexp"
)

type ID_ string
var validID *regexp.Regexp = regexp.MustCompile(`^[a-z0-9_]+$`)

func (i ID_) validate() error {
	if !validID.MatchString(i) {
		return fmt.Errorf("invalid ID_: %s", i)
	}
	return nil
}
