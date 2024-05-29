package semantics

type Expr [3]exprPart
type ExprPart struct {
	S         string
	Is_quoted bool
}
type Part int
const (
	Database Part = iota
	Schema
	Table
)

func CreateRegexpIdentifier(s string) *regexp.Regexp {
	s = strings.ReplaceAll(s, "$", "\\$") // escape dollar sign, which can be used in Snowflake identifiers
	s = strings.ReplaceAll(s, "*", ".*")  // transform the wildcard suffix into a zero or more regular expression
	s = "(?i)^" + s + "$"                 // match case insensitive; match complete identifier
	return regexp.MustCompile(s)
}

func (e exprPart) MatchAll() bool {
	return !e.is_quoted && e.s == "*"
}

var validUnquotedExpr *regexp.Regexp = regexp.MustCompile(`^[a-z_][a-z0-9_$]{0,254}\*?$`) // lowercase identifier chars + optional wildcard suffix
var validQuotedExpr *regexp.Regexp = regexp.MustCompile(`.{0,255}`)

func (lhs expr) subsetOf(rhs expr) bool {
	// return true if rhs can match at least all objects that lhs can match
	if !lhs[_database].subsetOf(rhs[_database]) {
		return false
	}
	if !lhs[_schema].subsetOf(rhs[_schema]) {
		return false
	}
	return lhs[_table].subsetOf(rhs[_table])
}

func (lhs exprPart) subsetOf(rhs exprPart) bool {
	// return true if rhs can match at least all objects that lhs can match
	if lhs.is_quoted && rhs.is_quoted {
		return lhs.s == rhs.s // also return true if improper subset
	}
	if !lhs.is_quoted && rhs.is_quoted {
		return false // unqoted will always match more objects than quoted
	}
	if lhs.is_quoted && !rhs.is_quoted {
		re := CreateRegexpIdentifier(rhs.s)
		return re.MatchString(lhs.s)
	}
	// !lhs.isquoted && !rhs.is_quoted
	if lhs.s == rhs.s {
		return true
	}
	if !strings.ContainsRune(lhs.s, '*') && !strings.ContainsRune(rhs.s, '*') {
		return false
	}
	// strings are not equal and at least one of them contains a wildcard suffix
	if !strings.ContainsRune(rhs.s, '*') {
		return false
	}
	// rhs.s contains a wildcard suffix; lhs.s may or may not contain one
	if !strings.ContainsRune(lhs.s, '*') {
		if len(lhs.s) < len(rhs.s)-1 {
			return false
		}
		return lhs.s[0:len(rhs.s)-1] == rhs.s[0:len(rhs.s)-1]
	}
	// both lhs.s and rhs.s contain a wildcard suffix
	return strings.HasPrefix(lhs.s[0:len(lhs.s)-1], rhs.s[0:len(rhs.s)-1])
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

func (m map[expr]bool) allDisjoint() bool {
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

func (lhs expr) disjoint(rhs expr) bool {
	if lhs[_database].disjoint(rhs[_database]) {
		return true
	}
	if lhs[_schema].disjoint(rhs[_schema]) {
		return true
	}
	return lhs[_table].disjoint(rhs[_table])
	// TODO implement tests
	// *.*.*	whatever	!disjoint
	// a.*.*	b.*.*		disjoint
	// a.*.c	a.b.c		!disjoint
	// a.*.c	a.b.d		disjoint
	// ...
}

func (lhs exprPart) disjoint(rhs exprPart) bool {
	// return true if no object can be matched by both lhs and rhs
	return !lhs.subsetOf(rhs) && !rhs.subsetOf(lhs)
	// we can't have an intersection with both sets also still having
	// a non empty complement cause we only allow a suffix wildcard
}

func parseObjExpr(s string) (expr, error) {
	var empty expr // for return statements that have an error
	if strings.ContainsRune(s, '\n') {
		return empty, fmt.Errorf("object expression has newline")
	}
	r := csv.NewReader(strings.NewReader(s)) // encoding/csv can conveniently handle quoted parts
	r.Comma = '.'
	record, err := r.Read()
	if err != nil {
		return empty, fmt.Errorf("reading csv: %s", err)
	}
	if len(record) != 3 {
		return empty, fmt.Errorf("object expression does not have three parts")
	}
	var expr expr
	// figure out which parts were quoted, if any
	for i, substr := range record {
		expr[i].s = substr
		_, start := r.FieldPos(i)
		start = start - 1 // FieldPos columns start numbering from 1
		if s[start] == '"' {
			// this is a quoted field
			end := start + 1 + len(substr)
			if end == len(s) || s[end] != '"' {
				panic("did not find quote at end of parsed quoted CSV field")
			}
			expr[i].is_quoted = true
		} else {
			// this is an unquoted field
			end := start + len(substr)
			if end != len(s) && s[end] != '.' {
				panic("unquoted field not ending with end of line or period")
			}
		}
	}
	// validate identifier expressions
	for _, exprPart := range expr {
		if !exprPart.is_quoted && !validUnquotedExpr.MatchString(exprPart.s) {
			return empty, fmt.Errorf("not a valid unquoted identifier matching expression")
		}
		if exprPart.is_quoted && !validQuotedExpr.MatchString(exprPart.s) {
			return empty, fmt.Errorf("not a valid quoted identifier matching expression")
		}
	}
	// expecting only one line, just checking there was not more
	_, err = r.Read()
	if err != io.EOF {
		panic("parsing obj expr did not result in single result")
	}
	return expr, nil
}