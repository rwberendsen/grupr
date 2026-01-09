package snowflake

import (
	"context"
	"database/sql"
	"fmt"
	"iter"
	"maps"
	"strings"
)

type GrantToRole struct {
	Privilege 		Privilege
	CreateObjectType	ObjType // "": not applicable (privilege != prvCreate)
	GrantedOn 		ObjType
	Name 			string
	GrantOption		bool
	GrantedBy		string
}

func NewGrantToRole(privilege string, createObjType string, grantedOn string, name string,
		grantOption bool, grantedBy string) GrantToRole {
	return GrantToRole{
		Privilege: parsePrivilege(privilege),
		CreateObjectType: parseObjType(createObjType),
		GrantedOn: parseObjType(grantedOn),
		Name: name,
		GrantOption: grantOption,
		GrantedBy: grantedBy,
	}
}

func QueryGrantsToRoleFiltered(ctx context.Context, conn *sql.DB, role string,
		privileges map[Privilege]struct{}, createObjTypes map[ObjType]struct{}) iter.Seq2[GrantToRole, error] {
	return queryGrantsToRole(ctx, conn, "", role, privileges, createObjTypes)
}

func QueryGrantsToDBRoleFiltered(ctx context.Context, conn *sql.DB, db string, role string,
		privileges map[Privilege]struct{}, createObjTypes map[ObjType]struct{}) iter.Seq2[GrantToRole, error] {
	return queryGrantsToRole(ctx, conn, db, role, privileges, createObjTypes)
}

func QueryGrantsToRole(ctx context.Context, conn *sql.DB, role string) iter.Seq2[GrantToRole, error] {
	return queryGrantsToRole(ctx, conn, "", role, nil, nil)
}

func QueryGrantsToDBRole(ctx context.Context, conn *sql.DB, db string, role string) iter.Seq2[GrantToRole, error] {
	return queryGrantsToRole(ctx, conn, db, role, nil, nil)
}

func buildSQL(db string, role string, privileges map[Privilege]struct{}, createObjTypes map[ObjType]struct{}) (sql string, param string) {
	// fetch grants for DATABASE ROLE if needed, rather than ROLE
	var dbClause string
	param = role
	if db != "" {
		dbClause = `DATABASE `
		param = fmt.Sprintf(`"%s".%s`, db, role)
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

return
}

func queryGrantsToRole(ctx context.Context, conn *sql.DB, db string, role string,
		privileges map[Privilege]struct{}, createObjTypes map[ObjType]struct{}) iter.Seq2[GrantToRole, error] {
	sql, param := buildSQL(db, role, privileges, createObjTypes)
	return func(yield func(GrantToRole, error) bool) {
		rows, err := conn.QueryContext(ctx, sql, param)
		defer rows.Close()
		if err != nil { 
			yield(GrantToRole{}, err)
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
				yield(GrantToRole{}, err)
				return
			}
			if !yield(GrantToRole{
				privilege: privilege,
				createObjType: 
				grantedOn: grantedOn,
				name: name,
				grantOption: grantOption,
				grantedBy string,
			}, nil) {
				return
			}
		}
		if err = rows.Err(); err != nil {
			yield(GrantToRole{}, err)
			return
		}
	}
}
