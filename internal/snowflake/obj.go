package snowflake

import (
	"context"
	"database/sql"
	"iter"

	"github.com/rwberendsen/grupr/internal/semantics"
)

type Obj struct {
	Name       semantics.Ident
	ObjectType ObjType
	Owner      semantics.Ident
}

func QueryObjs(ctx context.Context, conn *sql.DB, db semantics.Ident, schema semantics.Ident, mots map[ObjType]bool) iter.Seq2[Obj, error] {
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
