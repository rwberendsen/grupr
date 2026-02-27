package semantics

import (
	"encoding/csv"
	"fmt"
	"io"
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
	var isQuoted bool
	if strings.HasPrefix(s, `"`) {
		if len(s) < 3 || !strings.HasSuffix(s, `"`) {
			return Ident(""), fmt.Errorf("invalid quoted identifier string")
		}
		isQuoted = true
		reader := csv.NewReader(strings.NewReader(s)) // encoding/csv can conveniently handle quoted parts,
		// same way we use it everywhere else
		reader.FieldsPerRecord = 1
		if rec, err := reader.Read(); err != nil {
			return Ident(""), fmt.Errorf("reading csv: %s", err)
		} else {
			s = rec[0]
		}
		// expecting only one record, just checking there was not more
		if _, err := reader.Read(); err != io.EOF {
			panic("parsing did not result in single record")
		}
	}
	return NewIdent(cnf, s, isQuoted)
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
