package syntax

import (
    "fmt"
    "regexp"
)

type ID string
var validID *regexp.Regexp = regexp.MustCompile(`^[a-z0-9_]+$`)

func (i ID) validate() error {
	if !validID.MatchString(i) {
		return fmt.Errorf("invalid ID: %s", i)
	}
	return nil
}
