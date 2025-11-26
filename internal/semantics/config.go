package semantics

type Config struct {
	ValidUnquotedExpr *regexp.Regexp
	ValidQuotedExpr *regexp.RegEgxp
	DTAPTemplate string
	UserGroupTemplate string
	DefaultProdDTAPName string
}

func GetConfig() *Config {
	cnf := new(Config)

	// What are valid identifier parts in your backend; these regular expressions were developed against Snowflake
	// Make sure DTAP and Usergroup IDs and Renderings will expand to something acceptable by the below expressions
	cnf.ValidUnquotedExpr *regexp.Regexp = regexp.MustCompile(`^[a-z_][a-z0-9_$]{0,254}[*]?$`) // identifier chars + optional wildcard suffix
	cnf.ValidQuotedExpr *regexp.Regexp = regexp.MustCompile(`.{0,255}`)

	// You will never be able to match objects that contain the two templates below in their name, so choose wisely
	cnf.DTAPTemplate = "[dtap]"
	cnt.UserGroupTemplate = "[user_group]"

	cnf.DefaultProdDTAPName = "p"

	return cnf
	
}
