package snowflake

import (
	"github.com/rwberendsen/grupr/internal/semantics"
)

type Interface {
	Matched Matched
}

func newInterface(i semantics.Interface, c *accountCache) Interface {
	// lazily reads which objects exist in Snowflake and adds them to c, modifying c
	return Interface{newMatched(i.Matcher, c)}
}