package semantics

type ObjExprAttr struct {
	DTAP      string // Empty string means this data product only has a production DTAP
	UserGroup string // Empty string means shared
}

func nDTAPsObjExprAttr(m map[ObjExprAttr]struct{}) int {
	dtaps := map[string]struct{}{}
	for ea := range m {
		dtaps[ea.DTAP] = struct{}{}
	}
	return len(dtaps)
}

func nUGsObjExprAttr(m map[ObjExprAttr]struct{}) int {
	ugs := map[string]struct{}{}
	for ea := range m {
		ugs[ea.UserGroup] = struct{}{}
	}
	return len(ugs)
}
