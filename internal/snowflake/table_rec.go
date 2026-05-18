package snowflake

import (
	"context"
	"database/sql"
	"fmt"
	"iter"
	"strings"

	"github.com/rwberendsen/grupr/internal/semantics"
)

type tableRec struct {
	name            semantics.Ident
	owner           semantics.Ident
	is_external     string
	owner_role_type string
	is_event        string
	is_hybrid       string
	is_iceberg      string
	is_dynamic      string
	is_immutable    string // should always be N anyway, since we filter out temporary tables, but, never mind, could change in the future.
	is_interactive  string
}

func (r tableRec) getObjType(mots map[ObjType]bool) ObjType {
	// First, exclude any object not owned by a role
	// Fortunately, in Snowflake, until now OWNERSHIP and CREATE cannot be granted to users yet (May 2026)
	// See: https://docs.snowflake.com/en/sql-reference/sql/grant-privilege-user
	if ParseObjType(r.owner_role_type) != ObjTpRole {
		return ObjTpOther
	}
	// Now, start accepting objects of types we manage
	if r.is_external == "N" &&
		r.is_hybrid == "N" &&
		r.is_event == "N" &&
		r.is_iceberg == "N" &&
		r.is_dynamic == "N" &&
		r.is_immutable == "N" &&
		r.is_interactive == "N" {
		return ObjTpTable
	}
	if r.is_external == "N" &&
		r.is_hybrid == "Y" &&
		r.is_event == "N" &&
		r.is_iceberg == "N" &&
		r.is_dynamic == "N" &&
		r.is_immutable == "N" &&
		r.is_interactive == "N" {
		return ObjTpHybridTable
	}
	// Else, return ObjTpOther, meaning this object type is not managed by grupr
	return ObjTpOther
}

func queryTables(ctx context.Context, conn *sql.DB, db semantics.Ident, schema semantics.Ident) iter.Seq2[tableRec,
	error] {
	// The main problem with the SHOW TABLES function is that there is no flag "is_normal" for regular tables.
	// If new types of tables are returned in the future, with flags like "is_new_type_X", "is_new_type_Y",
	// grupr will treat it like a regular table. That means grupr may crash, for example, when it tries to
	// grant SELECT ERROR TABLE, which, as of Apr 2026, is only applicable to normal tables.
	//
	// Until Snowflake adds a flag "is_normal" to the output of the SHOW TABLES function (and friends like SHOW OBJECTS,
	// SHOW VIEWS, etc, I see no easy way to prevent this problem, other than quickly fixing grupr each time Snowflake
	// comes out with something new (again).
	return func(yield func(tableRec, error) bool) {
		// When there are more than 10K results, paginate.
		// Because we apply filters, even if fewer results are returned, perhaps there are still more.
		// For that reason, our last row has a count of the first query result
		mayHaveMore := true
		var fromClause string
		limit := 10000
		for mayHaveMore {
			rows, err := conn.QueryContext(ctx, fmt.Sprintf(`SHOW TABLES IN SCHEMA IDENTIFIER($$%s.%s$$) LIMIT %d%s ->>
SELECT
    NULL AS n
  , "name" AS name
  , "owner" AS owner
  , "is_external" AS is_external
  , "owner_role_type" AS owner_role_type
  , "is_event" AS is_event
  , "is_hybrid" AS is_hybrid
  , "is_iceberg" AS is_iceberg
  , "is_dynamic" AS is_dynamic
  , "is_immutable" AS is_immutable
  , "is_interactive" AS is_interactive
FROM $1
WHERE
    kind IN ('TABLE', 'TRANSIENT')
AND owner_role_type = '%s'
AND is_external = 'N'
AND is_event = 'N'
AND is_hybrid = 'N'
AND is_iceberg = 'N'
AND is_dynamic = 'N'
AND is_immutable = 'N'
AND is_interactive = 'N'
UNION ALL
SELECT
    COUNT(*)
  , '' AS name
  , '' AS owner
  , '' AS is_external
  , '' AS owner_role_type
  , '' AS is_event
  , '' AS is_hybrid
  , '' AS is_iceberg
  , '' AS is_dynamic
  , '' AS is_immutable
  , '' AS is_interactive
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
				var rec tableRec
				if err = rows.Scan(
					&n,
					&rec.name,
					&rec.owner,
					&rec.is_external,
					&rec.owner_role_type,
					&rec.is_event,
					&rec.is_hybrid,
					&rec.is_iceberg,
					&rec.is_dynamic,
					&rec.is_immutable,
					&rec.is_interactive,
				); err != nil {
					err = fmt.Errorf("queryTables: error scanning row: %w", err)
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
				err = fmt.Errorf("queryTables: error after looping over results: %w", err)
				yield(tableRec{}, err)
				return
			}
		}
	}
}
