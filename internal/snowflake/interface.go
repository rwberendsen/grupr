package snowflake

import (
	"github.com/rwberendsen/grupr/internal/semantics"
)

type Interface struct {
	AccountObjects map[semantics.ObjExpr]*AccountObjs
	GrantsTo struct{}
	GrantsOn struct{}
	GrantsOf struct{}
}

func newInterfaceFromMatched(m map[semantics.ObjExpr]*matchedAccountObjects, oms semantics.ObjMatchers) *Interface {
	i := &Interface{AccountObjects: map[semantics.ObjExpr]*AccountObjs{},}
	for e, om := range oms {
		tmpAccountObjs = newAccountObjsFromMatched(m[e])
		i.AccountObjects[e] = newAccountObjects(tmpAccountObjs, e, om)
	}
	return i
}

func newInterface(m map[semantics.ObjExpr]*AccountObjects, oms semantics.ObjMatchers) *Interface {
	i := &Interface{AccountObjects: map[semantics.ObjExpr]*AccountObjs{},}
	for e, om := range oms {
		i.AccountObjects[e] = newAccountObjects(m[om.SubsetOf])
	}
	return i
}
