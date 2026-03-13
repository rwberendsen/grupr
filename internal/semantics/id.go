package semantics

import (
	"fmt"
	"regexp"
	"slices"
)

// TODO: Consider introducing yet another string based type for this
// type ID string

func NewID(cnf *Config, s string) (string, error) {
	if !cnf.ValidID.MatchString(s) || !cnf.ValidUnquotedExpr.MatchString(s) {
		return id, &FormattingError{fmt.Sprintf("invalid ID: '%s'", s)}
	}
	/*
		"Is there a suffix on the ID that can be used with a prefix of the infix to form a complete infix?
		And, equivalently, is there a prefix on the ID that can be used with a suffix of the infix to form a complete infix?"

		Concat: part + infix + part; and validate that there is exactly one infix present on a single position in the string

		If so, it means this ID can cause no ambiguity when parsing a concatenation of it with other IDs as to what are the parts.

		Note that, before concatenating, ID would be uppercased in ANSI SQL compatible databases, and hence we must do the 
		same in this test. We do this by first converting ID to an Ident.
	*/
	i := NewIdentUnquoted(s)
	cat := i + cnf.Infix + i
	catRunes := []rune(cat)
	infixRunes := []rune(cnf.Infix)
	matches := []int{}
	for i := 0; i <= len(catRunes)-len(infixRunes); i++ {
		if slices.Compare(catRunes[i:i+len(infixRunes)], infixRunes) == 0 {
			matches = append(matches, i)
		}
	}
	if len(matches) != 1 {
		return s, &FormattingError{fmt.Sprintf("invalid ID when used with infix: '%s'", s)}
	}
	return s, nil
}
