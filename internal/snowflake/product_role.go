package snowflake

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"strings"

	"github.com/rwberendsen/grupr/internal/syntax"
)

type ProductRole struct {
	ProductID string
	DTAP string
	Mode Mode
	ID string
}

func newProductRole(synCnf *syntax.Config, cnf *Config, productID string, dtap string, mode Mode) ProductRole) {
	return ProductRole{
		ProductID: productID,
		DTAP: dtap,
		Mode: mode,
		ID: strings.ToUpper(synCnf.Prefix + productID + cnf.Infix + dtap + cnf.Infix + mode),
	}
}

func newProductRoleFromString(synCnf *syntax.Config, cnf *Config, role string) (ProductRole, error) {
	r := ProductRole{ID: role,}
	if !role.HasPrefix(cnf.Prefix) { return r, fmt.Errorf("role does not start with Grupr prefix: '%s'", r.ID) }
	role = strings.TrimPrefix(role, cnf.Prefix)
	parts := strings.Split(role, synCnf.Infix)
	if len(parts) != 3 { return r, fmt.Errorf("role does not have three parts: '%s'", r.ID) }
	r.ProductID = strings.ToLower(parts[0])
	r.DTAP = strings.ToLower(parts[1])
	if r.Mode, err := parseMode(strings.ToLower(parts[2])); err != nil { return r, fmt.Errorf("invalid role: '%s': %w", r.ID, err) }
	if mode != Read { return r, fmt.Errorf("unimplemented mode '%s' for role '%s'", mode, role) }
	return r, nil
}

func (r ProductRole) Create(ctx context.Context, cnf *Config, conn *sql.DB) error {
	if err := runSQL(ctx, cnf, conn, `CREATE ROLE IF NOT EXISTS IDENTIFIER(?)`, r.ID); err != nil { return err }
	if err := runSQL(ctx, cnf, conn `GRANT ROLE IDENTIFIER(?) TO ROLE SYSADMIN`, r.ID); err != nil { return err }
	return nil
}

func (r ProductRole) hasUnmanagedPrivileges(ctx context.Context, cnf *Config, conn *sql.DB) (bool, error) {
	// TODO put the privileges in cnf
	for grant, err := range QueryGrantsToRoleFilteredLimit(ctx, conn, r.ID, nil,
		map[Grant]struct{}{
			Grant{
				Privilege: PrvUsage,
				GrantedOn: ObjTpDatabaseRole,
				GrantedBy: cnf.Role,
			}: {},
		},
		1) {
		if err != nil { return true, err }
		return true, nil
	}
	return false, nil
}

func (r ProductRole) Drop(ctx context.Context, cnf *Config, conn *sql.DB) error {
	if has, err := r.hasUnmanagedPrivileges(ctx, cnf, conn); err != nil {
		return err
	} else if has {
		log.Printf("role %v has privileges not managed by Grupr, skipping dropping\n", r)
		return nil
	}
	return runSQL(ctx, cnf, conn, `DROP ROLE IF EXISTS IDENTIFIER(?)`, r.ID)
}

func (r ProductRole) String() string {
	return r.ID
}
