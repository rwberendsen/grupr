package snowflake

import (
	"github.com/rwberendsen/grupr/internal/semantics"
)

type Interface struct {
	Matched Matched
}

func newInterface(i semantics.InterfaceMetadata, c *accountCache) Interface {
	// lazily reads which objects exist in Snowflake and adds them to c, modifying c
	return Interface{newMatched(i.ObjMatcher, c)}
}
