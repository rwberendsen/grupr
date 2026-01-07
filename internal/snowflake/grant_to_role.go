package snowflake

import (
	"context"
	"database/sql"
	"fmt"
	"iter"
	"strings"
)

type grantToRole struct {
	privilege 	Privilege
	createObjectType *ObjectType // nil: not applicable (not a create privilege)
	grantedOn 	ObjectType
	name 		string
	grantOption	bool
	grantedBy	string
}

func queryGrantsToRole(ctx context.Context, conn *sql.DB, role string, isDatabaseRole bool, privileges []Privilege) iter.Seq2[grantToRole, error] {
	var dbClause string
	if isDatabaseRole {
		dbClause = `DATABASE `
	}
	privilegesStr := make([]string, len(privileges))
	for i, p := range privileges {
		privilegesStr[i] = fmt.Sprintf("%v", p)
	}
	privilegesStrJoin := strings.Join(privilegesStr, ", ")
	var sql
	return func(yield func(grantToRole, error) bool) {
		rows, err := conn.QueryContext(ctx, fmt.Sprintf(`SHOW GRANTS TO %sROLE IDENTIFIER(?)
->> SELECT
    "privilege"		AS privilege
  , "granted_on"	AS granted_on
  , "name"		AS name
  , "grant_option"	AS grant_option
  , "granted_by"	AS granted_by
FROM $1
WHERE privilege IN (%s)`, dbClause, privilegesStr), role)
		defer rows.Close()
		if err != nil { 
			yield(grantToRole{}, err)
			return
		}
		for rows.Next() {
			var privilege Privilege
			var grantedOn ObjectType
			var name string
			var grantOption bool
			var grantedBy string
			if err = rows.Scan(&privilege, &grantedOn, &name, &grantOption, &grantedBy); err != nil {
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
