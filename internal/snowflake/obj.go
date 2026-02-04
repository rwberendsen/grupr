package snowflake

import (
	"context"
	"database/sql"
	"fmt"
	"iter"
)

type Obj struct {
	Name       string
	ObjectType ObjType
	Owner      string
}

func QueryObjs(ctx context.Context, conn *sql.DB, db string, schema string) iter.Seq2[Obj, error] {
	return func(yield func(Obj, error) bool) {
		// When there are more than 10K results, paginate.
		// Because we apply filters, even if fewer results are returned, perhaps there are still more.
		// For that reason, our last row has a count of the first query result
		mayHaveMore := true
		var fromClause string
		limit := 10000
		for mayHaveMore {
			rows, err := conn.QueryContext(ctx, fmt.Sprintf(`SHOW OBJECTS IN SCHEMA IDENTIFIER(?) LIMIT %d%s ->>
SELECT
    NULL AS n
  , "name" AS name
  , "kind" As kind
  , "owner" AS owner
FROM S1 WHERE kind in (?, ?)
UNION ALL
SELECT
    COUNT(*)
  , '' AS name
  , '' AS kind
  , '' AS owner
FROM $1
`, limit, fromClause), quoteIdentifier(dbName)+"."+quoteIdentifier(schemaName), ObjTpTable.String(), ObjTpView.String())
			if err != nil {
				if strings.Contains(err.Error(), "390201") { // ErrObjectNotExistOrAuthorized; this way of testing error code is used in errors_test in the gosnowflake repo
					err = ErrObjectNotExistOrAuthorized
				}
				yield(Obj{}, err)
				return
			}
			var lastName string
			for rows.Next() {
				var n *int
				var name string
				var kind string
				var owner string
				if err = rows.Scan(&n, &name, &kind, &owner); err != nil {
					err = fmt.Errorf("QueryObjs: error scanning row: %w", err)
					yield(Obj, err)
					return
				}
				if n != nil { // this is the last row holding the count
					if n < limit {
						mayHaveMore = false
					} else {
						fromClause = fmt.Sprintf(" FROM '%s'", lastName)
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
			rows.Close()
		}
	}
}
