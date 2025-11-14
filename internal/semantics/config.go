package semantics

type Config struct {
	var validUnquotedExpr *regexp.Regexp = regexp.MustCompile(`^[a-z_][a-z0-9_$]{0,254}[*]?$`) // identifier chars + optional wildcard suffix
	var validQuotedExpr *regexp.Regexp = regexp.MustCompile(`.{0,255}`)
}

func GetConfig() *Config {
	cnf := new(Config)

	// What are valid identifier parts in your backend; these regular expressions were developed against Snowflake
	cnf.ValidUnquotedExpr *regexp.Regexp = regexp.MustCompile(`^[a-z_][a-z0-9_$]{0,254}[*]?$`) // identifier chars + optional wildcard suffix
	cnf.ValidQuotedExpr *regexp.Regexp = regexp.MustCompile(`.{0,255}`)

	// You will never be able to match objects that contain the two templates below in their name, so choose wisely
	cnf.DTAPTemplate = "[dtap]"
	cnt.UserGroupTemplate = "[user_group]"

	return cnf
	
}
