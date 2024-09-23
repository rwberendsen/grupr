package syntax

import (
    "fmt"
    "regexp"
)

var validID *regexp.Regexp = regexp.MustCompile(`^[a-z0-9_]+$`)

func validateID(i string) error {
	if !validID.MatchString(i) {
		return fmt.Errorf("invalid ID: %s", i)
	}
	return nil
}
