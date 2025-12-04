package snowflake

import (
	"fmt"
)

type dbKind int

const (
	StandardDatabase dbKind = iota
	ImportedDatabase
)

type dbKey struct {
	name string
	kind dbKind
}

func getDBKind(k string) dbKind {
	if k == 'STANDARD' { return StandardDatabase }
	if k == 'IMPORTED DATABASE' { return ImportedDatabase }
	panic("Unsupported database kind")
}

func (k dbKey) String() {
	fmt.Println("%s (%s)", k.name, k.kind)
}
