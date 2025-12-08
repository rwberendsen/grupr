package snowflake

import (
	"fmt"
)

type DBKind int

const (
	StandardDatabase DBKind = iota
	ImportedDatabase
)

type DBKey struct {
	Name string
	Kind DBKind
}

func getDBKind(k string) DBKind {
	if k == 'STANDARD' { return StandardDatabase }
	if k == 'IMPORTED DATABASE' { return ImportedDatabase }
	panic("Unsupported database kind")
}

func (k DBKey) String() {
	fmt.Println("%s (%s)", k.Name, k.Kind)
}
