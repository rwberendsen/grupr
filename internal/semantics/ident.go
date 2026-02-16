package semantics

import (
	"strings"
)

type Ident string

func NewIdent(s string, isQuoted bool) (Ident, error) {
	var id Ident
	if isQuoted {
		if !cnf.ValidQuotedExpr.MatchString(s) {
			return id, fmt.Errorf("invalid quoted identifier string")
		}
		id = s
	} else {
		if !cnf.ValidUnquotedExpr.MatchString(s) {
			return id, fmt.Errorf("invalid unquoted identifier string")
		}
		id = strings.ToUpper(s)
	}
}

func (i Ident) Quote() string {
	i = strings.ReplaceAll(i, `"`, `""`)
	return `"` + s + `"`
}

func (i Ident) String() string {
	return i.Quote()
}
