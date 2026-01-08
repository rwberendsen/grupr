package snowflake

import (
	"context"
	"database/sql"
	"fmt"
	"iter"
	"maps"
	"strings"
)

type grantToRole struct {
	privilege 	Privilege
	createObjType   *ObjectType // nil: not applicable (not a create privilege)
	grantedOn 	ObjectType
	name 		string
	grantOption	bool
	grantedBy	string
}

func newGrantToRole(privilege string, createObjType *string, grantedOn string, name string,
		grantOption bool, grantedBy string) error {
	r := grantToRole{
		name: name,
		grantOption: grantOption,
		grantedBy: grantedBy,
	}
	if p, err := newPrivilege(privilege); err != nil {
		return err
	} else {
		r.privilege = p
	} // WIP
}

func queryGrantsToRoleFiltered(ctx context.Context, conn *sql.DB, role string,
		privileges map[Privilege]struct{}, createObjTypes map[ObjectType]struct{}) iter.Seq2[grantToRole, error] {
	return queryGrants(ctx, conn, role, false, privileges, createObjTypes)
}

func queryGrantsToDBRoleFiltered(ctx context.Context, conn *sql.DB, role string,
		privileges map[Privilege]struct{}, createObjTypes map[ObjectType]struct{}) iter.Seq2[grantToRole, error] {
	return queryGrants(ctx, conn, role, true, privileges, createObjTypes)
}

func queryGrantsToRole(ctx context.Context, conn *sql.DB, role string, isDatabaseRole bool,
		privileges map[Privilege]struct{}, createObjTypes map[ObjectType]struct{}) iter.Seq2[grantToRole, error] {
	// fetch grants for DATABASE ROLE if needed, rather than ROLE
	var dbClause string
	if isDatabaseRole {
		dbClause = `DATABASE `
	}

	// Check if we need to separately handle a CREATE privilege, restricting it to certain object types
	var createPrvlgClause string
	if _, ok := privileges[Create]; ok {
		if len(createObjTypes) > 0 {
			objTypeStr := []string{}
			for k := range maps.Keys(createObjTypes) {
				objTypeStr = append(objTypeStr, fmt.Sprintf("%v", p))
			}
			objTypeStrJoin := strings.Join(objTypeStr, ", ")
			createPrvlgClause = fmt.Sprintf(`
(
      privilege = 'CREATE'
  AND create_obj_type IN (%s)
)`, objTypesStrJoin)
		}
	}

	// Do the remaining privileges, if any
	var prvlgClause string
	prvlgs := map[Privilege]struct{} // copy, cause we want to delete an item in here possibly
	for k := range privileges {
		prvlgs[k] = struct{}{}
	}
	if len(createPrvlgClause) > 0 { // we have handled the CREATE privilege in a seperate clause
		delete(prvlgs, Create) // not nice to delete on argument passed in by caller
	}
	if len(prvlgs) > 0 { // there are remaining privileges to include
		privilegesStr := []string{}
		for k := range prvlgs.Keys() {
			privilegesStr = append(privilegesStr, fmt.Sprintf("%v", p))
		}
		privilegesStrJoin := strings.Join(privilegesStr, ", ")
		prvlgClause = fmt.Sprintf(`privilege IN (%s)`, privlegesStrJoin)
	}

	var whereClause string
	if len(prvlgClause) > 0 && len(createPrvlgClause) > 0 {
		whereClause = fmt.Sprintf(`WHERE %s
OR%s
`, prvlgClause, createPrvlgClause)
	} else if len(prvlgClause) > 0 {
		whereClause = fmt.Sprintf(`WHERE %s`, prvlgClause)
	} else if len(createPrvlgClause) > 0 {
		whereClause = fmt.Sprintf(`WHERE%s`, createPrvlgClause)
	}

	var sql string
	sql := fmt.Sprintf(`SHOW GRANTS TO %sROLE IDENTIFIER(?)
->> SELECT
  , CASE
    WHEN STARTSWITH("privilege", 'CREATE ') THEN 'CREATE'
    ELSE "privilege"
    END AS privilege
  , CASE
    WHEN STARTSWITH("privilege", 'CREATE ') THEN SUBSTR("privilege", 8)
    ELSE NULL
    END AS create_obj_type
  , "granted_on"	AS granted_on
  , "name"		AS name
  , "grant_option"	AS grant_option
  , "granted_by"	AS granted_by
FROM $1
%s`, dbClause, whereClause)

	return func(yield func(grantToRole, error) bool) {
		rows, err := conn.QueryContext(ctx, sql, role)
		defer rows.Close()
		if err != nil { 
			yield(grantToRole{}, err)
			return
		}
		for rows.Next() {
			var privilege string
			var createObjType *string
			var grantedOn string
			var name string
			var grantOption bool
			var grantedBy string
			if err = rows.Scan(&privilege, &creatObjType, &grantedOn, &name, &grantOption, &grantedBy); err != nil {
				yield(grantToRole{}, err)
				return
			}
			if !yield(grantToRole{
				privilege: privilege,
				createObjectType: 
				grantedOn: grantedOn,
				name: name,
				grantOption: grantOption,
				grantedBy string,
			}, nil) {
				return
			}
		}
		if err = rows.Err(); err != nil {
			yield(grantToRole{}, err)
			return
		}
	}
}
