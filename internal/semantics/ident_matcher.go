package semantics

import (
	"fmt"
	"strings"
)

type IdentMatcher struct {
	S           Ident
	HasWildcard bool
	isQuoted    bool
	matchAll    bool
}

func NewIdentMatcher(cnf *Config, s string, isQuoted bool) (IdentMatcher, error) {
	idm := IdentMatcher{isQuoted: isQuoted}
	if len(s) == 0 {
		return idm, fmt.Errorf("empty ident matcher expression")
	}
	if !isQuoted {
		// Unquoted identifier matching expressions are only supposed
		// to have '*' at the end, just take it if it is there.  if
		// there are more of those, or other non-identifier characters,
		// it will be caught within NewIdent
		if strings.HasSuffix(s, `*`) {
			idm.HasWildcard = true
			s = s[0 : len(s)-1]
		}
	} else {
		// Quoted identifiers may themselves contain all sorts of printable
		// characters, including '*'. So, here, we ask that other '*' characters
		// would be "escaped" by doubling them.
		// We return the string without the wildcard suffix (if it had one),
		// and with all instances of '**' replaced by '*'
		if stripped, hasWildcard, err := stripWildcardQuotedIdentMatcher(s); err != nil {
			return idm, err
		} else {
			s = stripped
			idm.HasWildcard = hasWildcard
		}
	}
	// s now has had a wildcard suffix ('*') stripped, if there was one.
	if len(s) == 0 {
		if !(idm.HasWildcard) {
			panic("unexpected condition: empty expression")
		}
		idm.matchAll = true
	} else {
		// after stripping the optional wildcard, we still have a string left,
		// this is an identifier, for all practical purposes, unquoted or quoted.
		if ident, err := NewIdent(cnf, s, isQuoted); err != nil {
			return idm, err
		} else {
			idm.S = ident
		}
	}
	return idm, nil
}

func NewMatchAllIdentMatcher() IdentMatcher {
	return IdentMatcher{HasWildcard: true, matchAll: true}
}

func normalizeAsterisksQuotedIdentMatcher(s string) string {
	return strings.ReplaceAll(s, `**`, `*`)
}

func stripWildcardQuotedIdentMatcher(s string) (stripped string, hasWildcard bool, err error) {
	// We will walk through s to find '*' characters, skipping them if they are doubled,
	// and only accept an unescaped '*' character if it appears at the end of s.
	remainder := s
	for len(remainder) > 0 {
		switch i := strings.IndexRune(remainder, '*'); i {
		case -1:
			// There is no '*' in the remainder, prepare to exit the for loop
			remainder = ""
		case len(s) - 1:
			// There is a '*' character, and it is in the final position
			// Note that we rely on our knowledge here that '*' is a single bit character
			hasWildcard = true
			// Strip the wildcard suffix of of s
			s = s[:i]
			remainder = ""
		default:
			// There is a '*' character, but it is not in the final position
			if s[i+1:i+2] == "*" {
				// It is directly followed by another '*'
				// character though, proceed past both of them
				// with the search
				remainder = s[i+2:]
			} else {
				err = fmt.Errorf("unescaped '*' inside expression")
			}
		}
	}
	stripped = normalizeAsterisksQuotedIdentMatcher(s)
	return
}

func (im IdentMatcher) Match(i Ident) bool {
	if !im.HasWildcard {
		return im.S == i
	}
	if im.matchAll {
		return true
	}
	return strings.HasPrefix(string(i), string(im.S))
}

func (lhs IdentMatcher) subsetOf(rhs IdentMatcher) bool {
	// return true if rhs can match at least all objects that lhs can match
	if rhs.MatchAll() {
		return true
	}
	if lhs.MatchAll() {
		return false
	}
	if rhs.HasWildcard {
		return strings.HasPrefix(string(lhs.S), string(rhs.S))
	}
	if lhs.HasWildcard {
		return false
	}
	return lhs.S == rhs.S
}

func (lhs IdentMatcher) disjoint(rhs IdentMatcher) bool {
	// return true if no object can be matched by both lhs and rhs
	return !lhs.subsetOf(rhs) && !rhs.subsetOf(lhs)
	// we can't have an intersection with both sets also still having
	// a non empty complement cause we only allow a suffix wildcard
}

func (im IdentMatcher) MatchAll() bool {
	return im.matchAll
}

func (im IdentMatcher) String() string {
	s := strings.ReplaceAll(string(im.S), `*`, `**`)
	s = strings.ReplaceAll(s, `"`, `""`)
	if im.HasWildcard {
		s += "*"
	}
	if im.isQuoted {
		s = `"` + s + `"`
	}
	return s
}
