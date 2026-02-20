package semantics

import (
	"regexp"
)

type Config struct {
	ValidUnquotedExpr   *regexp.Regexp
	ValidQuotedExpr     *regexp.Regexp
	DefaultProdDTAPName string
}

func GetConfig() *Config {
	cnf := new(Config)

	// What are valid identifier parts in your backend; these regular expressions were developed against Snowflake
	// Make sure DTAP and Usergroup IDs and Renderings will expand to something acceptable by the below expressions
	cnf.ValidUnquotedExpr = regexp.MustCompile(`^[a-zA-Z_][a-zA-Z0-9_$]{0,254}$`) // identifier chars + optional wildcard suffix
	cnf.ValidQuotedExpr = regexp.MustCompile(`^.{1,255}$`)
	cnf.DefaultProdDTAPName = "p"

	return cnf

}
