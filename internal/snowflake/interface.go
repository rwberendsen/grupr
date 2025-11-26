package snowflake

import (
	"github.com/rwberendsen/grupr/internal/semantics"
)

type Interface struct {
	Matched Matched
}

func newInterface(i semantics.InterfaceMetadata) *Interface {
	return &Interface{newMatched(i.ObjectMatchers)}
}

func refreshInterface() {
	return
}
