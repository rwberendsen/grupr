package snowflake

import (
	"context"
	"database/sql"
	"fmt"
	"iter"
	"strings"

	"github.com/rwberendsen/grupr/internal/semantics"
)

type Obj struct {
	Name       semantics.Ident
	ObjectType ObjType
	Owner      semantics.Ident
}

func GetObjs(ctx context.Context, conn *sql.DB, db semantics.Ident, schema semantics.Ident, map[ObjType]bool mots) iter.Seq2[Obj, error] {
	return func(yield func(Obj, error) bool) {
		for rec := range queryTables(ctx, conn, db, schema) {
			if ot := rec.getObjType(mots); ot != ObjTpOther {
				if !yield(Obj{Name: rec.name, ObjectType: ot, Owner: rec.owner}, nil) {
						return
				}
			}
		}
		for rec := range queryViews(ctx, conn, db, schema) {
			if ot := rec.getObjType(mots); ot != ObjTpOther {
				if !yield(Obj{Name: rec.name, ObjectType: ot, Owner: rec.owner}, nil) {
						return
				}
			}
		}
	}
}
