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

func QueryObjs(ctx context.Context, conn *sql.DB, db semantics.Ident, schema semantics.Ident) iter.Seq2[Obj, error] {
	return func(yield func(Obj, error) bool) {
		// When there are more than 10K results, paginate.
		// Because we apply filters, even if fewer results are returned, perhaps there are still more.
		// For that reason, our last row has a count of the first query result
		mayHaveMore := true
		var fromClause string
		limit := 10000
		for mayHaveMore {
			rows, err := conn.QueryContext(ctx, fmt.Sprintf(`SHOW OBJECTS IN SCHEMA IDENTIFIER('%s.%s') LIMIT %d%s ->>
SELECT
    NULL AS n
  , "name" AS name
  , "kind" As kind
  , "owner" AS owner
FROM $1 WHERE kind in ('%s', '%s')
UNION ALL
SELECT
    COUNT(*)
  , '' AS name
  , '' AS kind
  , '' AS owner
FROM $1
`, db, schema, limit, fromClause, ObjTpTable, ObjTpView))
			if err != nil {
				if strings.Contains(err.Error(), "390201") { // ErrObjectNotExistOrAuthorized; this way of testing error code is used in errors_test in the gosnowflake repo
					err = ErrObjectNotExistOrAuthorized
				}
				yield(Obj{}, err)
				return
			}
			defer rows.Close()
			var lastName semantics.Ident
			for rows.Next() {
				var n *int
				var name semantics.Ident
				var kind string
				var owner semantics.Ident
				if err = rows.Scan(&n, &name, &kind, &owner); err != nil {
					err = fmt.Errorf("QueryObjs: error scanning row: %w", err)
					yield(Obj{}, err)
					return
				}
				if n != nil { // this is the last row holding the count
					if *n < limit {
						mayHaveMore = false
					} else {
						fromClause = fmt.Sprintf(" FROM '%s'", string(lastName))
					}
					continue
				}
				obj := Obj{Name: name, ObjectType: ParseObjType(kind), Owner: owner}
				if !yield(obj, nil) {
					return
				}
				lastName = name
			}
			if err = rows.Err(); err != nil {
				err = fmt.Errorf("QueryObjs: error after looping over results: %w", err)
				yield(Obj{}, err)
				return
			}
		}
	}
}
