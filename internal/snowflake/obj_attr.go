package snowflake

type ObjAttr struct {
	ObjectType 	ObjType
	Owner 		string
	GrantsTo	map[Mode]map[Privilege]struct{} // has to be unique for every interface; hence for every accountobjs; we can't copy it around
}

func (o ObjAttr) setGrantTo(m Mode, p Privilege) {
	if o.GrantsTo == nil { o.GrantsTo = map[Mode]map[Privilege]struct{}{} }
	if _, ok := o.GrantsTo[m]; !ok { o.GrantsTo[m] = map[Privilege]struct{}{} }
	o.GrantsTo[m][p] = struct{}{}
}
