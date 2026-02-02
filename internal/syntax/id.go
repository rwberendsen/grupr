package syntax

import (
	"fmt"
	"regexp"
	"slices"
)

var validID *regexp.Regexp = regexp.MustCompile(`^[a-z0-9_]+$`)

func validateID(i string) error {
	if !validID.MatchString(i) {
		return &FormattingError{fmt.Sprintf("invalid ID: '%s'", i)}
	}
	return nil
}

func validateIDPart(cnf *Config, s string) error {
	if err := validateID(s); err != nil {
		return err
	}
	/*
		"Is there a suffix on the part that can be used with a prefix of the infix to form a complete infix?
		And, equivalently, is there a prefix on the part that can be used with a suffix of the infix to form a complete infix?"

		Concat: part + infix + part; and validate that there is exactly one infix present on a single position in the string
	*/
	cat := s + cnf.Infix + s
	catRunes := []rune(cat)
	infixRunes := []rune(cnf.Infix)
	matches := []int{}
	for i := 0; i <= len(catRunes)-len(infixRunes); i++ {
		if slices.Compare(catRunes[i:i+len(infixRunes)], infixRunes) == 0 {
			matches = append(matches, i)
		}
	}
	if len(matches) != 1 {
		return &FormattingError{fmt.Sprintf("invalid ID part when used with infix: '%s'", s)}
	}
	return nil
}
