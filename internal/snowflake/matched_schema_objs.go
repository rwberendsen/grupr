package snowflake

import (
	"github.com/rwberendsen/grupr/internal/semantics"
)

type matchedSchemaObjs struct {
	version int
	objects map[semantics.Ident]ObjAttr
}
