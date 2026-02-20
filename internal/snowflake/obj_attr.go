package snowflake

import (
	"github.com/rwberendsen/grupr/internal/semantics"
)

type ObjAttr struct {
	ObjectType ObjType
	Owner      semantics.Ident
}
