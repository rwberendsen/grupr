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

type objRec struct {
	name semantics.Ident
	kind string
	owner semantics.Ident
	is_hybrid bool
	is_dynamic bool
	is_iceberg bool
	is_interactive bool
}

func newObj(name semantics.Ident, objType ObjType, owner semantics.Ident) (Obj, error) {
	if len(name) == 0 {
		return Obj{}, fmt.Errorf("zero length identifier")
	}
	if objType != ObjTpTable && objType != ObjTpView {
		panic("ObjTp not implemented")
	}
	return Obj{Name: name, ObjectType: objType, Owner: owner}, nil
}

func ParseObjTypeFromShowObjectsRecord(rec objRec) (ObjType, bool) {
	switch rec.kind {
	case "TABLE":
		// TODO: there appears to be something like a dynamic iceberg table, for example, not sure how it would be
		// represented here, and what would be the set of privileges we can assign an object like that, it is not
		// separate treated in the Snowflake documentation page on access control privileges as of April 2026
		if rec.is_hybrid && !rec.is_dynamic && !rec.is_iceberg && !rec.is_interactive {
			return ObjTpHybridTable, true
		}
		if !rec.is_hybrid && rec.is_dynamic && !rec.is_iceberg && !rec.is_interactive {
			return ObjTpDynamicTable, true
		}
		if !rec.is_hybrid && !rec.is_dynamic && rec.is_iceberg && !rec.is_interactive {
			return ObjTpIcebergTable, true
		}
		if !rec.is_hybrid && !rec.is_dynamic && !rec.is_iceberg && rec.is_interactive {
			return ObjTpInteractiveTable, true
		}
	case "ONLINE_FEATURE_TABLE":
		return ObjTpOnlineFeatureTable, true
	case "EVENT_TABLE": // TODO: validate this is how it appears in the output
		return ObjTpEventTable, true
	case "EXTERNAL_TABLE": // TODO: validate this is how it appears in the output
		return ObjTpExternalTable, true
	case "VIEW":
		return ObjTpView, false // we cannot fully determine object type
	}
	return ObjTpOther, false
}

func GetObjs(ctx context.Context, conn *sql.DB, db semantics.Ident, schema semantics.Ident) iter.Seq2[Obj, error] {
	return func(yield func(Obj, error) bool) {
		toDetermine := map[string]objRec{}
		for rec := range queryObjs(ctx, conn, db, schema) {
				if objType, ok := ParseObjTypeFromRecord(kind, is_hybrid, is_dynamic, is_iceberg, is_interactive); ok {
					if !yield(Obj{Name: name, ObjectType: objType, Owner: owner}, nil) {
							return
					}
				} else {
					toDetermine[name] = objRec{
						kind: kind,
						is_hybrid: is_hybrid,
						is_dynamic: is_dynamic,
						is_iceberg: is_iceberg,
						is_interactive: is_interactive,
					}
				}
		}
		if toDetermineHasViews() > 0 {
			// There were objects for which we cannot decide the object type based on the output of SHOW OBJECTS 
			// Still, we need to know the object type, to be able to manage grants correctly
			// The only practical way appears to be to query again, for more specific object types. 
			// In particular, when kind equals VIEW, we need to know if it is a regular view, a materialized view,
			// or a semantic view. SHOW VIEWS does not appear to include semantic views.
			// We may query specifically for MATERIALIZED VIEWS, and then SEMANTIC VIEWS.
			// And, any remaining objects with kind VIEW we may assume to be regular views--until Snowflake introduces
			// yet more view types without indicating so in SHOW OBJECTS its output
			
			// WIP: fire query for materialized views, and delete matching records from toDetermine
			// WIP: if toDetermine still has views, fire query for semantic views, and delete matching records from toDetermine
			// WIP: yield any remaining views in toDetermine as regular views
		}
	}
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
