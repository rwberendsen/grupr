package semantics

import (
	"regexp"
)

type Config struct {
	ValidQuotedExpr     *regexp.Regexp
	ValidUnquotedExpr   *regexp.Regexp
	ValidID             *regexp.Regexp
	Prefix              Ident
	Infix               Ident
	DefaultProdDTAPName string
}

func GetConfig() (*Config, error) {
	cnf := &Config{
		// What are valid identifier parts in your backend; these regular expressions were developed against Snowflake
		// Make sure DTAP and Usergroup IDs and Renderings will expand to something acceptable by the below expressions
		ValidQuotedExpr:   regexp.MustCompile(`^.{1,255}$`),
		ValidUnquotedExpr: regexp.MustCompile(`^[a-zA-Z_][a-zA-Z0-9_$]{0,254}$`), // identifier chars + optional wildcard suffix

		// What are valid product id's, dtap names, usergroup names?
		// NB: its important to keep accepting only lower case id's: currently, that's the approach,
		// when we parse, e.g., role identifiers as seen in ANSI SQL compatible databases, we lowercase those id's
		//
		// NB2: In addition whatever we do, if something matches ValidID, it should also match ValidUnquotedExpr
		// We don't want to have to deal with quotes in there.
		ValidID: regexp.MustCompile(`^[a-z0-9_]+$`),

		// If no DTAPs are specified in a product or service account, by default you will get only a production DTAP with
		// the name specified here.
		DefaultProdDTAPName: "p",
	}

	// With what prefix would you like to distinguish objects (e.g., roles) that are managed by Grupr in your database platform?
	if pfx, err := NewIdentStripQuotesIfAny(`_x_`, cnf.ValidQuotedExpr, cnf.ValidUnquotedExpr); err != nil {
		return cnf, err
	} else {
		cnf.Prefix = pfx
	}

	// With what infix would you build roles names that contain product ids, dtaps, and user groups?
	if ifx, err := NewIdentStripQuotesIfAny(`_x_`, cnf.ValidQuotedExpr, cnf.ValidUnquotedExpr); err != nil {
		return cnf, err
	} else {
		cnf.Infix = ifx
	}

	return cnf, nil
}
