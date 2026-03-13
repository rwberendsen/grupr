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

func GetConfig() *Config {
	cnf := new(Config)

	// What are valid identifier parts in your backend; these regular expressions were developed against Snowflake
	// Make sure DTAP and Usergroup IDs and Renderings will expand to something acceptable by the below expressions
	cnf.ValidQuotedExpr = regexp.MustCompile(`^.{1,255}$`)
	cnf.ValidUnquotedExpr = regexp.MustCompile(`^[a-zA-Z_][a-zA-Z0-9_$]{0,254}$`) // identifier chars + optional wildcard suffix

	// What are valid product id's, dtap names, usergroup names?
	// NB: its important to keep accepting only lower case id's: currently, that's the approach,
	// when we parse, e.g., role identifiers as seen in ANSI SQL compatible databases, we lowercase those id's
	//
	// NB2: In addition whatever we do, if something matches ValidID, it should also match ValidUnquotedExpr
	// We don't want to have to deal with quotes in there.
	cnf.ValidID = regexp.MustCompile(`^[a-z0-9_]+$`)

	// With what prefix would you like to distinguish objects (e.g., roles) that are managed by Grupr in your database platform?
	cnf.Prefix = NewIdentStripQuotesIfAny("_x_", cnf.ValidQuotedExpr, cnf.ValidUnquotedExpr)
	// With what infix would you build roles names that contain product ids, dtaps, and user groups?
	cnf.Infix  = NewIdentStripQuotesIfAny("_x_", cnf.ValidQuotedExpr, cnf.ValidUnquotedExpr)

	return cnf

}
