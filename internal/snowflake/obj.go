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

func GetObjs(ctx context.Context, conn *sql.DB, db semantics.Ident, schema semantics.Ident) iter.Seq2[Obj, error] {
	return func(yield func(Obj, error) bool) {
		for rec := range queryTables(ctx, conn, db, schema) {
				if ot := rec.getObjType() ; ot != ObjTpOther {
					if !yield(Obj{Name: rec.name, ObjectType: ot, Owner: rec.owner}, nil) {
							return
					}
				}
		}
		if toDetermineHasViews() > 0 {
			// There were objects for which we cannot decide the object type based on the output of SHOW OBJECTS 
			// Still, we need to know the object type, to be able to manage grants correctly
			// The only practical way appears to be to query again, for more specific object types. 
			// In particular, when kind equals VIEW, we need to know if it is a regular view, a materialized view,
			// or a semantic view. (SHOW VIEWS does not appear to include semantic views, but SHOW OBJECTS should).
			// We may query specifically for MATERIALIZED VIEWS, and then SEMANTIC VIEWS.
			// And, any remaining objects with kind VIEW we may assume to be regular views--until Snowflake introduces
			// yet more view types without indicating so in SHOW OBJECTS its output
			// Oh boy, SHOW MATERIALIZED VIEWS does not offer paging beyond 10K results, unlike SHOW VIEWS.
			// Inconsistencies, inconsistencies. SHOW SEMANTIC VIEWS does support paging, but it does not include
			// semantic views; at least, it does not say so in the docs.
			// So I guess we could use SHOW VIEWS to identify >10K materialized views, we'd have to filter out the
			// SN_bla_bla Snowpark objects. We'd then have to separately query SEMANTIC VIEWS still, if we wanted to
			// support those, too. If we query SHOW VIEWS, then it makes sense not to query them with SHOW OBJECTS at
			// all, actually.
			// And once we get there, it may make more sense to use SHOW TABLES, where we get a bit more flags as well.
			
			// WIP: fire query for materialized views, and delete matching records from toDetermine
			// WIP: if toDetermine still has views, fire query for semantic views, and delete matching records from toDetermine
			// WIP: yield any remaining views in toDetermine as regular views
		}
	}
}

func queryViews(ctx context.Context, conn *sql.DB, db semantics.Ident, schema semantics.Ident) iter.Seq2[viewRec,
	error] {
	// The main problem with the SHOW VIEWS function is that there is no flag "is_normal" for regular views.
	// If new types of views are returned in the future, with flags like "is_new_type_X", "is_new_type_Y",
	// grupr will treat it like a regular view.
	// Until Snowflake adds a flag "is_normal" to the output of the SHOW VIEWS function (and friends like SHOW OBJECTS,
	// SHOW TABLES, etc, I see no easy way to prevent this problem, other than quickly fixing grupr each time Snowflake
	// comes out with something new (again).
}

func queryObjs(ctx context.Context, conn *sql.DB, db semantics.Ident, schema semantics.Ident) iter.Seq2[objRec, error] {
	// The problem with the SHOW OBJECTS function is that there is no flag "is_regular" for regular tables.
	// If new types of tables are returned in the future, with flags like "is_new_type_X", "is_new_type_Y",
	// calling code will treat it like a regular table.
	return func(yield func(Obj, error) bool) {
		// When there are more than 10K results, paginate.
		// Because we apply filters, even if fewer results are returned, perhaps there are still more.
		// For that reason, our last row has a count of the first query result
		mayHaveMore := true
		var fromClause string
		limit := 10000
		for mayHaveMore {
			rows, err := conn.QueryContext(ctx, fmt.Sprintf(`SHOW OBJECTS IN SCHEMA IDENTIFIER($$%s.%s$$) LIMIT %d%s ->>
SELECT
    NULL AS n
  , "name" AS name
  , "kind" AS kind
  , "is_hybrid" AS is_hybrid
  , "is_dynamic" AS is_dynamic
  , "is_iceberg" AS is_iceberg
  , "is_interactive" AS is_interactive
  , "owner" AS owner
FROM $1
WHERE kind = '%s'
UNION ALL
SELECT
    COUNT(*)
  , '' AS name
  , '' AS kind
  , FALSE AS is_hybrid
  , FALSE AS is_dynamic
  , FALSE AS is_iceberg
  , FALSE AS is_interactive
  , '' AS owner
FROM $1
`, db, schema, limit, fromClause, ObjTpTable))
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
				var rec objRec
				if err = rows.Scan(&n, &rec.name, &rec.kind, &rec.is_hybrid, &rec.is_dynamic, &rec.is_iceberg,
					&rec.is_interactive, &rec.owner); err != nil {
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
				if !yield(rec, nil) {
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
