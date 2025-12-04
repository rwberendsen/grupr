package snowflake

import (
	"fmt"
)

type objKind int

const (
	Table objKind = iota
	View
)

type objKey struct {
	name string
	kind objKind
}

func getObjKind(k string) objKind {
	if k == 'TABLE' { return Table }
	if k == 'VIEW' { return View }
	panic("Unsupported object kind")
}

func (k objKey) String() {
	fmt.Println("%s (%s)", k.name, k.kind)
}
