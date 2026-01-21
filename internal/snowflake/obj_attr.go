package snowflake

import (
	"context"
	"database/sql"
)

type ObjAttr struct {
	ObjectType 	ObjType
	Owner 		string
}
