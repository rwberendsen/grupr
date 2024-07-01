package semantics

import (
	"regexp"
	"strings"
)

var validUnquotedExpr *regexp.Regexp = regexp.MustCompile(`^[a-z_][a-z0-9_$]{0,254}[*]?$`) // identifier chars + optional wildcard suffix
var validQuotedExpr *regexp.Regexp = regexp.MustCompile(`.{0,255}`)

func CreateRegexpIdentifier(s string) *regexp.Regexp {
	s = strings.ReplaceAll(s, "$", "\\$") // escape dollar sign, which can be used in Snowflake identifiers
	s = strings.ReplaceAll(s, "*", ".*")  // transform the wildcard suffix into a zero or more regular expression
	s = "(?i)^" + s + "$"                 // match case insensitive; match complete identifier
	return regexp.MustCompile(s)
}

type ExprPart struct {
	S        string
	IsQuoted bool
}

func (lhs ExprPart) subsetOf(rhs ExprPart) bool {
	// return true if rhs can match at least all objects that lhs can match
	if lhs.IsQuoted && rhs.IsQuoted {
		return lhs.S == rhs.S // also return true if improper subset
	}
	if !lhs.IsQuoted && rhs.IsQuoted {
		return false // unqoted will always match more objects than quoted
	}
	if lhs.IsQuoted && !rhs.IsQuoted {
		re := CreateRegexpIdentifier(rhs.S)
		return re.MatchString(lhs.S)
	}
	// !lhs.Isquoted && !rhs.IsQuoted
	if lhs.S == rhs.S {
		return true
	}
	if !strings.ContainsRune(lhs.S, '*') && !strings.ContainsRune(rhs.S, '*') {
		return false
	}
	// strings are not equal and at least one of them contains a wildcard suffix
	if !strings.ContainsRune(rhs.S, '*') {
		return false
	}
	// rhs.s contains a wildcard suffix; lhs.s may or may not contain one
	if !strings.ContainsRune(lhs.S, '*') {
		if len(lhs.S) < len(rhs.S)-1 {
			return false
		}
		return lhs.S[0:len(rhs.S)-1] == rhs.S[0:len(rhs.S)-1]
	}
	// both lhs.s and rhs.s contain a wildcard suffix
	return strings.HasPrefix(lhs.S[0:len(lhs.S)-1], rhs.S[0:len(rhs.S)-1])
	// TODO implement tests, e.g.:
	// abc	abc*	subset
	// abc	ab*	subset
	// abc*	abc	!subset
	// abc*	ab*	subset
	// ab*	abc*	!subset
	// ab	abc*	!subset
	// a*	*	subset
	// *	a*	!subset
}

func validateExprPart(p ExprPart) bool {
	if p.IsQuoted {
		return validQuotedExpr.MatchString(p.S)
	}
	if !validUnquotedExpr.MatchString(p.S) {
		return p.S == "*"
	}
	return true
}

func (lhs ExprPart) disjoint(rhs ExprPart) bool {
	// return true if no object can be matched by both lhs and rhs
	return !lhs.subsetOf(rhs) && !rhs.subsetOf(lhs)
	// we can't have an intersection with both sets also still having
	// a non empty complement cause we only allow a suffix wildcard
}

func (e ExprPart) MatchAll() bool {
	return !e.IsQuoted && e.S == "*"
}

func (e ExprPart) String() string {
	s := ""
	if e.IsQuoted {
		s += "\""
	}
	s += e.S
	if e.IsQuoted {
		s += "\""
	}
	return s
}
