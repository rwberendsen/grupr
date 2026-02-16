package semantics

type IdentMatcher struct {
	S           Ident
        HasWildcard bool
}

func NewIdentMatcher(cnf *Config, s string, isQuoted bool) (IdentMatcher, error) {
	var idm IdentMatcher
	if !isQuoted {
		if strings.EndsWith(s, '*') {
			idm.HasWildcard = true
			s = s[0:-1]
		}
	} else {
		s, idm.HasWildCard = stripWildcardQuotedIdentMatcher(s)
	}
	if ident, err := NewIdent(s, isQuoted); err != nil {
		return err
	} else {
		idm.S = ident
	}
	return idm, nil
}

func stripWildcardQuotedIdentMatcher(s) (ret string, hasWildcard bool) {
	// WIP if there is an unescaped (single) wildcard char at the end, return true
	// and also de-double ** sequences in s, cause these characters may appear in quoted identifiers
}

func (i IdentMatcher) Match(s string) bool {
	// WIP you know, this will replace ExprPart, and things will work a bit simpler and more like expected, and we will have wildcards in quoted expression parts, too.
}

func (i IdentMatcher) String() string {
	if i.IsQuoted
}
