package semantics

import (
	"fmt"
	"strings"
)

type Ident string

func NewIdent(cnf *Config, s string, isQuoted bool) (Ident, error) {
	var id Ident
	if isQuoted {
		if !cnf.ValidQuotedExpr.MatchString(s) {
			return id, fmt.Errorf("invalid quoted identifier string")
		}
		id = Ident(s)
	} else {
		if !cnf.ValidUnquotedExpr.MatchString(s) {
			return id, fmt.Errorf("invalid unquoted identifier string")
		}
		id = Ident(strings.ToUpper(s))
	}
	return id, nil
}

func NewIdentStripQuotesIfAny(cnf *Config, s string) (Ident, error) {
	if strings.HasPrefix(s, `"`) {
		if len(s) < 3 || !strings.HasSuffix(s, `"`) {
			return Ident(""), fmt.Errorf("invalid quoted identifier string")
		}
		return NewIdent(cnf, s[1:len(s)-1], true)
	}
	return NewIdent(cnf, s, false)
}

func NewIdentUnquoted(s string) Ident {
	return Ident(strings.ToUpper(s))
}

func (i Ident) Quote() string {
	s := strings.ReplaceAll(string(i), `"`, `""`)
	return `"` + s + `"`
}

func (i Ident) String() string {
	return i.Quote()
}
