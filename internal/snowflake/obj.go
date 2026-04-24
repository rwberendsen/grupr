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

func ParseObjTypeFromShowObjectsRecord(rec objRec) (ObjType, bool) {
	switch ot := ParseObjTypeFromRecord(rec.kind); ot {
	case ObjTpTable:
		// TODO: there appears to be something like a dynamic iceberg table, for example, not sure how it would be
		// represented here, and what would be the set of privileges we can assign an object like that, it is not
		// separate treated in the Snowflake documentation page on access control privileges as of April 2026
		// For now, if we have a table with both is_dynamic and is_iceberg set to true, we would say we don't
		// recognize this type of object, returning ObjTpOther, false
		//
		// How this will be treated, then, is we will keep it around in the accountObjs as ObjTpOther.
		// Later, when we find a grant on this object (where it may say granted_on = TABLE), and, say
		// we want to revoke this grant, then we would either:
		// - find it in accountObjs as ObjTpOther: meaning we find we don't manage this type of table yet; and 
		//   thus the grant is unmanaged, and we don't revoke it, we leave it.
		// - not find it: if the object was created after we refreshed objects. In this case, at the moment,
		//   we would actually revoke it. We may have to change this behaviour. If we did not find it, but
		//   it is there according to the grant, we don't know the type of the object.
		//
		// But if we change that behaviour, we might also not keep the object around in accountObjs at all
		// if it's a type we don't manage.
		// But changing that behaviour could be expensive though. Remember if we want to revoke it,
		// in many cases that means it's not matched in the YAML of this product. So we would not find any
		// of those in the accountobjs of the present product. And thus we would revoke hardly anything.
		//
		// So perhaps, rather than checking the accountObjs, we could be checking the accountCache directly,
		// which has all objects we found while looking for all objects simultaneously. Then, if we don't
		// find it, it means
		// - it was created after we checked; we can leave the grant intact; next run it will be addressed; OR
		// - it was already there, but we did not collect / recognize it during our querying for objects; it's not a
		// type we support; OR
		// - it does not match the YAML of any product. 
		// Is there a single action that is always correct here?
		// We don't want to revoke grants that sysadmins did on objects, not even if the objects are not matched by any
		// YAML.
		// 
		// But we do want to revoke if this is a privilege within the scope of grupr. That would mean both the
		// privilege and the object type fall within grupr it's scope.
		//
		// It seems that there is no way to dodge the necessity to follow up on revoke candidates: go back and
		// query what the object type is, check if it's an object type that grupr is normally managing.
		//
		// Revoking privileges on objects is done per product-dtap, in parallel. So it makes sense to turn to
		// the account cache again.
		//
		// After processing grants, we have a rough idea of object type (granted_on = TABLE) still can mean
		// many things. When we get to the revoking part, we can find all the schema's that have objects to
		// revoke, and create matching expressions perhaps; since they may be schema's that are not even
		// in the YAML for any product, and thus they may or may not be in the accountcache. We could of
		// course check if a revoke candidate is disjoint from the grupin or not. If it is, we could create
		// a new matching expression to find the info. More precisely, if the schema is disjoint from the
		// grupin, then we would have never collected the objects in that schema. Then we would need to
		// to match it. But if it is not disjoint, then it means we did already query objects in that schema.
		// In that case, we could work with potentially stale data, concluding that if the object is not
		// in the account cache, we can leave it for a later grupr run. And if the object is there, we
		// can learn about its type, check if we manage that type normally, and if so, revoke the privilege.
		// If we had created a new matching expression, what happens here, we need a matchedAccountObjs
		// object, and then we can just locate the object(s) in that one, and act accordingly.
		// Well, looking at the account cache again, we don't have to check beforehand if the objects
		// are there already or not, if the schema is disjoint from the YAML or not. We can just go in
		// there with a version of 0. If the accountcache has a newer version, we'll take it without
		// a query to Snowflake. If we match a database or schema that was never matched until now by
		// any matching expression, then it will result in a query, as expected.
		// The same goes for transferring ownership.
		//
		// In this case, if the object is not of a type we normally manage, we could leave it out of the
		// accountObjs and friends. Then, if we don't find it, it can mean only that it was created after
		// we last checked, or, it is not a type we support (yet).
		// Alternatively, at the cost of some memory storage, we could keep it in accountobjs and friends,
		// and then we can log a more descriptive message on why we are not revoking a particular grant.
		// But if we go that, far, we might as well support it.
		//
		// So now I have to make a somewhat harder change, but, after that, I will be able to support
		// new fancy Snowflake object types when I want, and grupr can work correctly within its scope
		// always. The only caveat to that is that Snowflake makes it hard to identify "normal" tables.
		// They can introduce a new flag is_super_hip, and they may decide to use "TABLE" for the "kind"
		// column. I don't know about that flag, and I assume the table is normal, when in fact it is not.
		// In that case I will treat it like a normal table, and may end up attempting to grant privileges
		// that are not supported by super hip tables, in which case grupr will just crash. But that would
		// hardly be grupr it's problem.
		//
		// A technical thing is that we have no way yet of querying the account cache while indicating you
		// are not interested in writing, not even potentially. But that should be solvable.

		// We should also think about temporary tables: grants on those objects may pop up as well
		// we should probably leave them intact, cause grupr does not manage grants on temporary tables
		// so that means then that if we do not find a granted object in the account cache, then we leave
		// the grant intact. If we do find it and the object type is something grupr normally manages,
		// then we revoke.
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
	case ObjTpView:
		return ObjTpView, false // we cannot fully determine object type based on SHOW OBJECTS output
	default:
		return ot, true
	}
}

func GetObjs(ctx context.Context, conn *sql.DB, db semantics.Ident, schema semantics.Ident) iter.Seq2[Obj, error] {
	return func(yield func(Obj, error) bool) {
		// First, query table-like objects
		for rec := range queryObjs(ctx, conn, db, schema) {
				if ot, ok := ParseObjTypeFromShowObjectsRecord(rec); ok {
					if !yield(Obj{Name: name, ObjectType: ot, Owner: owner}, nil) {
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
