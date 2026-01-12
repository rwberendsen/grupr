package snowflake

import (
	"fmt"
)

type ObjKey struct {
	Name string
	ObjectType ObjType
}

func (k ObjKey) String() {
	fmt.Println("%s (%s)", k.Name, k.Kind)
}
