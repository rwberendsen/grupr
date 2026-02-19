package snowflake

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"strings"

	"github.com/rwberendsen/grupr/internal/semantics"
	"github.com/rwberendsen/grupr/internal/syntax"
)

type ProductRole struct {
	ProductID string
	DTAP      string
	Mode      Mode
	ID        semantics.Ident
}

func newProductRole(synCnf *syntax.Config, cnf *Config, productID string, dtap string, mode Mode) ProductRole {
	return ProductRole{
		ProductID: productID,
		DTAP:      dtap,
		Mode:      mode,
		ID:        semantics.NewIdentUnquoted(fmt.Sprintf("%s%s%s%s%s%s", cnf.ObjectPrefix, productID, synCnf.Infix, dtap, synCnf.Infix, mode)),
	}
}

func newProductRoleFromString(synCnf *syntax.Config, cnf *Config, role semantics.Ident) (ProductRole, error) {
	r := ProductRole{ID: role}
	roleStr := strings.ToLower(string(role))
	if !strings.HasPrefix(roleStr, cnf.ObjectPrefix) {
		return r, fmt.Errorf("role does not start with Grupr prefix: '%s'", r)
	}
	roleStr = strings.TrimPrefix(roleStr, cnf.ObjectPrefix)
	parts := strings.Split(roleStr, synCnf.Infix)
	if len(parts) != 3 {
		return r, fmt.Errorf("role does not have three parts: '%s'", r)
	}
	r.ProductID = parts[0]
	r.DTAP = parts[1]
	if mode, err := ParseMode(parts[2]); err != nil {
		return r, fmt.Errorf("invalid role: '%s': %w", r, err)
	} else if mode != ModeRead {
		return r, fmt.Errorf("unimplemented mode '%s' for role '%s'", mode, r)
	} else {
		r.Mode = mode
	}
	return r, nil
}

func (r ProductRole) Create(ctx context.Context, cnf *Config, conn *sql.DB) error {
	if err := runSQL(ctx, cnf, conn, `CREATE ROLE IF NOT EXISTS IDENTIFIER(?)`, r.String()); err != nil {
		return err
	}
	if err := runSQL(ctx, cnf, conn, `GRANT ROLE IDENTIFIER(?) TO ROLE SYSADMIN`, r.String()); err != nil {
		return err
	}
	return nil
}

func (r ProductRole) hasUnmanagedPrivileges(ctx context.Context, cnf *Config, conn *sql.DB) (bool, error) {
	for _, err := range QueryGrantsToRoleFilteredLimit(ctx, cnf, conn, r.ID, true, nil, cnf.ProductRolePrivileges[r.Mode], 1) {
		if err != nil {
			return true, err
		}
		return true, nil // there was an unmanaged grant, it does not matter what it was
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
	return runSQL(ctx, cnf, conn, `DROP ROLE IF EXISTS IDENTIFIER(?)`, r.String())
}

func (r ProductRole) String() string {
	return fmt.Sprintf("%v", r.ID)
}
