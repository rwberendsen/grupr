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
		if strings.EndsWith(s, '*') {
			idm.HasWildcard = true
			s = s[0:-1]
		}
	} else {
		s, idm.HasWildCard = stripWildcardQuotedIdentMatcher(s)
	}
	if len(s) == 0 {
		idm.matchAll = true
	else {
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

func stripWildcardQuotedIdentMatcher(s string) (ret string, hasWildcard bool) {
	remainder := s
	for len(remainder) > 0 {
		switch strings.IndexRune(remainder, '*') {
		case -1:
			remainder = ""
		case len(s) - 1:
			hasWildcard = true
			s = s[:-1] 
			remainder = ""
		default:
			if s[i+1:i+2] == "*"
			remainder = s[i+2:]
		}
	}
	s = normalizeAsterisksQuotedIdentMatcher(s)
	return
}

func (im IdentMatcher) Match(i Ident) bool {
	if !im.HasWildcard {
		return im.S == i
	} 
	if im.matchAll {
		return true
	}
	return strings.HasPrefix(i, im.S)
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
		return strings.HasPrefix(lhs.S, rhs.S)
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
	s := strings.ReplaceAll(im.S, `*`, `**`)
	s = strings.ReplaceAll(s, `"`, `""`)
	if im.HasWildcard {
		s+= "*"
	}
	if im.isQuoted {
		s = `"` + s + `"`
	}
	return s
}
