package snowflake

import (
	"fmt"
)

type ObjKind int

const (
	Table ObjKind = iota
	View
)

type ObjKey struct {
	Name string
	Kind ObjKind
}

func getObjKind(k string) ObjKind {
	if k == 'TABLE' { return Table }
	if k == 'VIEW' { return View }
	panic("Unsupported object kind")
}

func (k ObjKey) String() {
	fmt.Println("%s (%s)", k.Name, k.Kind)
}
