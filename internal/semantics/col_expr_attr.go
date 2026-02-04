package semantics

type ColExprAttr struct {
	DTAP      string // Empty means col expr omits DTAP template; it will be evaluated for all DTAPs
	UserGroup string // Empty means col expr omits UserGroup template; objects will be considered shared
}
