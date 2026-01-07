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
	mode Mode // R, O, W
	Database string
	ID string
}

func newDatabaseRole(synCnf *syntax.Config, cnf *Config, productID string, dtap string, interfaceID string, mode Mode, db string) DatabaseRole {
	r := DatabaseRole{
		ProductID: productID,
		DTAP: dtap,
		InterfaceID: interfaceID,
		mode: mode,
		Database: db,
	}
	if interfaceID == "" {
		r.ID = strings.ToUpper(synCnf.Prefix + productID + cnf.Infix + dtap + +cnf.Infix + fmt.Sprintf("%v", mode))
	} else {
		r.ID = strings.ToUpper(synCnf.Prefix + productID + cnf.Infix + dtap + +cnf.Infix + interfaceID + cnf.Infix + fmt.Sprintf("%v", mode))
	}
	return r
}

func newDatabaseRoleFromString(synCnf *syntax.Config, cnf *Config, role string, db string) (DatabaseRole, error) {
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

func (r DatabaseRole) grantToSelf(ctx context.Context, cnf *Config, conn *sql.DB) error {
	sql1 := `GRANT CREATE DATABASE ROLE ON DATABASE IDENTIFIER(?) TO ROLE (?)`
	param1 := r.Database
	param2 := cnf.Role
	if cnf.DryRun {
		printSQL(sql1, param1, param2)
		return nil
	}
	if _, err := conn.QueryContext(ctx, sql1, param1, param2); err != nil { return err }
	return nil
}

func (r DatabaseRole) create(ctx context.Context, cnf *Config, conn *sql.DB, grantSelf bool) error {
	if grantSelf {
		if err := r.grantToSelf(ctx, cnf, conn); err != nil { return err }
	}
	sql1 := `CREATE DATABASE ROLE IF NOT EXISTS IDENTIFIER(?)`
	param1 := r.Database
	if cnf.DryRun {
		printSQL(sql1, param1)
		return nil
	}
	if _, err := conn.QueryContext(ctx, sql1, param1, param2); err != nil { return err }
}

func (r DatabaseRole) String() string {
	return r.ID
}
