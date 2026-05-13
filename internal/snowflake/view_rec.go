package snowflake

import (
	"context"
	"database/sql"
	"fmt"
	"iter"
	"strings"

	"github.com/rwberendsen/grupr/internal/semantics"
)

type viewRec struct {
	name semantics.Ident
	owner semantics.Ident
	is_secure bool
	is_materialized bool
	owner_role_type ObjType
}

func (r viewRec) getObjType() ObjType {
	if ParseObjType(r.owner_role_type) != ObjTpRole {
		return ObjTpOther
	}
	// TODO WIP: recognize Materialize views as well
	if r.is_secure || r.is_materialized {
		return ObjTpOther
	}
	return ObjTpView
}

func queryViews(ctx context.Context, conn *sql.DB, db semantics.Ident, schema semantics.Ident) iter.Seq2[viewRec, error] {
	// The main problem with the SHOW VIEWS function is that there is no flag "is_normal" for regular views.
	// If new types of views are returned in the future, with flags like "is_new_type_X", "is_new_type_Y",
	// grupr will treat it like a regular view.
	// Until Snowflake adds a flag "is_normal" to the output of the SHOW VIEWS function (and friends like SHOW OBJECTS,
	// SHOW TABLES, etc, I see no easy way to prevent this problem, other than quickly fixing grupr each time Snowflake
	// comes out with something new (again).
	return func(yield func(viewRec, error) bool) {
		// When there are more than 10K results, paginate.
		// Because we apply filters, even if fewer results are returned, perhaps there are still more.
		// For that reason, our last row has a count of the first query result
		mayHaveMore := true
		var fromClause string
		limit := 10000
		for mayHaveMore {
			rows, err := conn.QueryContext(ctx, fmt.Sprintf(`SHOW VIEWS IN SCHEMA IDENTIFIER($$%s.%s$$) LIMIT %d%s ->>
SELECT
    NULL AS n
  , "name" AS name
  , "owner" AS owner
  , "is_secure" AS is_secure
  , "is_materialized" AS is_materialized
  , "owner_role_type" AS owner_role_type
FROM $1
WHERE
    owner_role_type = '%s'
AND NOT is_secure
AND NOT is_materialized
UNION ALL
SELECT
    COUNT(*)
  , '' AS name
  , '' AS owner
  , false AS is_secure
  , false AS is_materialized
  , '' AS owner_role_type
FROM $1
`, db, schema, limit, fromClause, ObjTpRole.RecordString()))
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
				var rec viewRec
				if err = rows.Scan(
					&n,
					&rec.name,
					&rec.owner,
					&rec.is_secure,
					&rec.is_materialized,
					&rec.owner_role_type,
				); err != nil {
					err = fmt.Errorf("queryViews: error scanning row: %w", err)
					yield(rec, err)
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
				if !yield(rec, nil) {
					return
				}
				lastName = rec.name
			}
			if err = rows.Err(); err != nil {
				err = fmt.Errorf("queryViews: error after looping over results: %w", err)
				yield(viewRec{}, err)
				return
			}
		}
	}
}
