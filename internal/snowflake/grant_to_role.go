package snowflake

import (
	"context"
	"database/sql"
	"iter"
)

type grantToRole struct {
	privilege 	Privilege
	grantedOn 	ObjectType
	name 		string
	grantOption	bool
	grantedBy	string
}

func queryGrantsToRole(ctx context.Context, conn *sql.DB, role string) iter.Seq2[grantToRole, error] {
	return func(yield func(grantToRole, error) bool) {
		rows, err := conn.QueryContext(ctx, `SHOW GRANTS TO ROLE IDENTIFIER(?) ->> SELECT "privilege", "granted_on", "name", "grant_option", "granted_by" FROM $1`, role)
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
