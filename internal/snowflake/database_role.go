package snowflake

import (
	"database/sql"
	"fmt"
	"strings"

	"github.com/rwberendsen/grupr/internal/syntax"
)

type DatabaseRole struct {
	ProductID string
	DTAP string
	InterfaceID string // "" means this is a product-level database role
	Mode Mode
	Database string
	ID string
}

func NewDatabaseRole(synCnf *syntax.Config, cnf *Config, productID string, dtap string, interfaceID string, mode Mode, db string) DatabaseRole {
	r := DatabaseRole{
		ProductID: productID,
		DTAP: dtap,
		InterfaceID: interfaceID,
		mode: mode,
		Database: db,
	}
	if interfaceID == "" {
		r.ID = strings.ToUpper(fmt.Sprintf("%s.%s%s%s%s%s%v", r.Database, synCnf.Prefix, productID, cnf.Infix, dtap, cnf.Infix, mode))
	} else {
		r.ID = strings.ToUpper(fmt.Sprintf("%s.%s%s%s%s%s%s%s%v", r.Database, synCnf.Prefix, productID, cnf.Infix, dtap, cnf.Infix, interfaceID, cnf.Infix, mode))
	}
	return r
}

func NewDatabaseRoleFromString(synCnf *syntax.Config, cnf *Config, db string, role string) (DatabaseRole, error) {
	r := DatabaseRole{ID: role, Database: db,}
	if !role.HasPrefix(cnf.Prefix) { return r, fmt.Errorf("role does not start with Grupr prefix: '%s'", r.ID) }
	role = strings.TrimPrefix(role, cnf.Prefix)
	parts := strings.Split(role, synCnf.Infix)
	if len(parts) != 3 && len(parts) != 4 { return r, fmt.Errorf("role does not have three or four parts: '%s'", r.ID) }
	r.ProductID = strings.ToLower(parts[0])
	r.DTAP = strings.ToLower(parts[1])
	posMode := 2
	if len(parts) == 4 {
		r.InterfaceID = strings.ToLower(parts[2])
		posMode += 1
	}
	if mode, err := parseMode(strings.ToLower(parts[posMode])); err != nil { return r, fmt.Errorf("invalid role: '%s': %w", r.ID, err) }
	if mode != Read { return r, fmt.Errorf("unimplemented mode '%s' for role '%s'", mode, role) }
	return r, nil
}

func QueryDatabaseRoles(ctx context.Context, synCnf *syntax.Config, cnf *Config, conn *sql.DB, db string) iter.Seq2[DatabaseRole, error]{
	return func(yield func(DatabaseRole, error) bool) {
		rows, err := conn.QueryContext(ctx, `SHOW DATABASE ROLES IN DATABASE IDENTIFIER(?)
	->> SELECT "name" FROM $1 WHERE "owner" = ? `, db.Name, strings.ToUpper(cnf.Role))
		defer rows.Close()
		if err != nil { 
			if strings.Contains(err.Error(), "390201") { // ErrObjectNotExistOrAuthorized; this way of testing error code is used in errors_test in the gosnowflake repo
				err = ErrObjectNotExistOrAuthorized
			}
			yield(DatabaseRole{}, err) 
			return
		}
		for rows.Next() {
			var roleName string
			if err = rows.Scan(&roleName); err != nil {
				yield(DatabaseRole{}, err}
				return
			}
			if r, err := newDatabaseRoleFromString(synCnf, cnf, db, roleName); err != nil {
				yield(DatabaseRole, err)
				return
			} else {
				if !yield(r, nil) { return }
			}
		}
		if err = rows.Err(); err != nil {
			yield(DatabaseRole{}, err)
		}
	}
}

func GrantCreateDatabaseRoleToSelf(ctx context.Context, cnf *Config, conn *sql.DB, db string) error {
	return runSQL(ctx, conn, `GRANT CREATE DATABASE ROLE ON DATABASE IDENTIFIER(?) TO ROLE (?)`, db, cnf.Role)
}

func (r DatabaseRole) Create(ctx context.Context, cnf *Config, conn *sql.DB) error {
	return runSQL(ctx, conn, `CREATE DATABASE ROLE IF NOT EXISTS IDENTIFIER(?)`, r.ID)
}

func (r DatabaseRole) String() string {
	return r.ID
}
