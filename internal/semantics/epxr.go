package semantics

import (
	"encoding/csv"
	"fmt"
	"io"
	"regexp"
	"strings"

	"golang.org/x/exp/maps"
)

type Exprs map[Expr]ExprAttr
type Expr [3]ExprPart
type ExprAttr struct {
	DTAP      string `yaml:"dtap,omitempty"`
	UserGroup string `yaml:"user_group,omitempty"`
}
type Part int

const (
	Database Part = iota
	Schema
	Table
)

const (
	DTAPTemplate      = "[dtap]"
	UserGroupTemplate = "[user_group]"
)

type ExprPart struct {
	S         string
	Is_quoted bool
}

func CreateRegexpIdentifier(s string) *regexp.Regexp {
	s = strings.ReplaceAll(s, "$", "\\$") // escape dollar sign, which can be used in Snowflake identifiers
	s = strings.ReplaceAll(s, "*", ".*")  // transform the wildcard suffix into a zero or more regular expression
	s = "(?i)^" + s + "$"                 // match case insensitive; match complete identifier
	return regexp.MustCompile(s)
}

func (e ExprPart) MatchAll() bool {
	return !e.Is_quoted && e.S == "*"
}

var validUnquotedExpr *regexp.Regexp = regexp.MustCompile(`^[a-z_][a-z0-9_$]{0,254}[*]?$`) // identifier chars + optional wildcard suffix
var validOrOperandChar *regexp.Regexp = regexp.MustCompile(`^[a-z0-9_$]$`)
var validQuotedExpr *regexp.Regexp = regexp.MustCompile(`.{0,255}`)

func (lhs Expr) subsetOfExprs(rhs Exprs) bool {
	for r := range rhs {
		if lhs.subsetOf(r) {
			return true
		}
	}
	return false
}

func (lhs Expr) subsetOf(rhs Expr) bool {
	// return true if rhs can match at least all objects that lhs can match
	if !lhs[Database].subsetOf(rhs[Database]) {
		return false
	}
	if !lhs[Schema].subsetOf(rhs[Schema]) {
		return false
	}
	return lhs[Table].subsetOf(rhs[Table])
}

func (lhs ExprPart) subsetOf(rhs ExprPart) bool {
	// return true if rhs can match at least all objects that lhs can match
	if lhs.Is_quoted && rhs.Is_quoted {
		return lhs.S == rhs.S // also return true if improper subset
	}
	if !lhs.Is_quoted && rhs.Is_quoted {
		return false // unqoted will always match more objects than quoted
	}
	if lhs.Is_quoted && !rhs.Is_quoted {
		re := CreateRegexpIdentifier(rhs.S)
		return re.MatchString(lhs.S)
	}
	// !lhs.Isquoted && !rhs.Is_quoted
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

func (m Exprs) allDisjoint() bool {
	keys := maps.Keys(m)
	if len(keys) < 2 {
		return true
	}
	for i := 0; i < len(keys)-1; i++ {
		for j := i + 1; j < len(keys); j++ {
			if !keys[i].disjoint(keys[j]) {
				return false
			}
		}
	}
	return true
}

func (lhs Expr) disjoint(rhs Expr) bool {
	if lhs[Database].disjoint(rhs[Database]) {
		return true
	}
	if lhs[Schema].disjoint(rhs[Schema]) {
		return true
	}
	return lhs[Table].disjoint(rhs[Table])
	// TODO implement tests
	// *.*.*	whatever	!disjoint
	// a.*.*	b.*.*		disjoint
	// a.*.c	a.b.c		!disjoint
	// a.*.c	a.b.d		disjoint
	// ...
}

func (lhs ExprPart) disjoint(rhs ExprPart) bool {
	// return true if no object can be matched by both lhs and rhs
	return !lhs.subsetOf(rhs) && !rhs.subsetOf(lhs)
	// we can't have an intersection with both sets also still having
	// a non empty complement cause we only allow a suffix wildcard
}

func validateExprPart(p ExprPart) bool {
	if p.Is_quoted {
		return validQuotedExpr.MatchString(p.S)
	}
	if !validUnquotedExpr.MatchString(p.S) {
		return p.S == "*"
	}
	return true
}

func newExpr(s string) (Expr, error) {
	r := Expr{}
	reader := csv.NewReader(strings.NewReader(s)) // encoding/csv can conveniently handle quoted parts
	reader.Comma = '.'
	record, err := reader.Read()
	if err != nil {
		return r, fmt.Errorf("reading csv: %s", err)
	}
	if len(record) != 3 {
		return r, fmt.Errorf("object expression does not have three parts")
	}
	// figure out which parts were quoted, if any
	for i, substr := range record {
		_, start := reader.FieldPos(i)
		start = start - 1 // FieldPos columns start numbering from 1
		if s[start] == '"' {
			// this is a quoted field
			end := start + 1 + len(substr)
			if end == len(s) || s[end] != '"' {
				panic("did not find quote at end of parsed quoted CSV field")
			}
			r[i].Is_quoted = true
			r[i].S = substr
		} else {
			// this is an unquoted field
			end := start + len(substr)
			if end != len(s) && s[end] != '.' {
				panic("unquoted field not ending with end of line or period")
			}
			r[i].S = strings.ToLower(substr) // unquoted identifiers match in a case insensitive way
		}
	}
	// validate identifier expressions
	for _, exprPart := range r {
		if !validateExprPart(exprPart) {
			return r, fmt.Errorf("invalid expr part: %s", exprPart.S)
		}
	}
	// expecting only one line, just checking there was not more
	if _, err := reader.Read(); err != io.EOF {
		panic("parsing obj expr did not result in single result")
	}
	return r, nil
}

func newExprs(s string, DTAPs map[string]bool, UserGroups map[string]bool) (Exprs, error) {
	exprs := Exprs{}
	if strings.ContainsRune(s, '\n') {
		return exprs, fmt.Errorf("object expression has newline")
	}
	dtapExpanded := map[string]ExprAttr{}
	if strings.Contains(s, DTAPTemplate) {
		if len(DTAPs) == 0 {
			return exprs, fmt.Errorf("expanding dtaps in '%s': no dtaps found", s)
		}
		for d := range DTAPs {
			dtapExpanded[strings.ReplaceAll(s, DTAPTemplate, d)] = ExprAttr{d, ""}
		}
	} else {
		dtapExpanded[s] = ExprAttr{"", ""}
	}
	userGroupExpanded := map[string]ExprAttr{}
	for k, v := range dtapExpanded {
		if strings.Contains(k, UserGroupTemplate) {
			if len(UserGroups) == 0 {
				return exprs, fmt.Errorf("expanding user groups in '%s': no user groups found", k)
			}
			for u := range UserGroups {
				userGroupExpanded[strings.ReplaceAll(k, UserGroupTemplate, u)] = ExprAttr{v.DTAP, u}
			}
		} else {
			userGroupExpanded[k] = ExprAttr{v.DTAP, ""}
		}
	}
	for k, v := range userGroupExpanded {
		expr, err := newExpr(k)
		if err != nil {
			return exprs, err
		}
		exprs[expr] = v
	}
	return exprs, nil
}
