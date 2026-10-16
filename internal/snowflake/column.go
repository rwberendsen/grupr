package snowflake

import (
	"github.com/rwberendsen/grupr/internal/semantics"
)

type Column struct {
	Name            semantics.Ident
	DataType        string
	OrdinalPosition int
}
