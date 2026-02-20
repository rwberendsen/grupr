package snowflake

import (
	"context"
	"database/sql"
	"fmt"
	"iter"
	"log"
	"strings"

	"github.com/rwberendsen/grupr/internal/semantics"
	"github.com/rwberendsen/grupr/internal/syntax"
)

type DatabaseRole struct {
	ProductID   string
	DTAP        string
	InterfaceID string // "" means this is a product-level database role
	Mode        Mode
	Database    semantics.Ident
	Name        semantics.Ident
}

func NewDatabaseRole(synCnf *syntax.Config, cnf *Config, productID string, dtap string, interfaceID string, mode Mode, db semantics.Ident) DatabaseRole {
	r := DatabaseRole{
		ProductID:   productID,
		DTAP:        dtap,
		InterfaceID: interfaceID,
		Mode:        mode,
		Database:    db,
	}
	if interfaceID == "" {
		r.Name = semantics.NewIdentUnquoted(fmt.Sprintf("%s%s%s%s%s%v", cnf.ObjectPrefix, productID, synCnf.Infix, dtap, synCnf.Infix, mode))
	} else {
		r.Name = semantics.NewIdentUnquoted(fmt.Sprintf("%s%s%s%s%s%s%s%v", cnf.ObjectPrefix, productID, synCnf.Infix, dtap, synCnf.Infix, interfaceID, synCnf.Infix, mode))
	}
	return r
}

func newDatabaseRoleFromString(synCnf *syntax.Config, cnf *Config, db semantics.Ident, role semantics.Ident) (DatabaseRole, error) {
	r := DatabaseRole{Name: role, Database: db}
	roleStr := strings.ToLower(string(role))
	if !strings.HasPrefix(roleStr, cnf.ObjectPrefix) {
		return r, fmt.Errorf("role does not start with Grupr prefix: '%s'", r)
	}
	roleStr = strings.TrimPrefix(roleStr, cnf.ObjectPrefix)
	parts := strings.Split(roleStr, synCnf.Infix)
	if len(parts) != 3 && len(parts) != 4 {
		return r, fmt.Errorf("role does not have three or four parts: '%s'", r)
	}
	r.ProductID = parts[0]
	r.DTAP = parts[1]
	posMode := 2
	if len(parts) == 4 {
		r.InterfaceID = parts[2]
		posMode += 1
	}
	if mode, err := ParseMode(parts[posMode]); err != nil {
		return r, fmt.Errorf("invalid role: '%s': %w", r, err)
	} else if mode != ModeRead {
		return r, fmt.Errorf("unimplemented mode '%s' for role '%s'", mode, r)
	}
	return r, nil
}

func QueryDatabaseRoles(ctx context.Context, synCnf *syntax.Config, cnf *Config, conn *sql.DB, db semantics.Ident) iter.Seq2[DatabaseRole, error] {
	return func(yield func(DatabaseRole, error) bool) {
		rows, err := conn.QueryContext(ctx, fmt.Sprintf(`SHOW DATABASE ROLES IN DATABASE IDENTIFIER('%s')
	->> SELECT "name" FROM $1 WHERE "owner" = '%s'`, db, cnf.Role))
		if err != nil {
			if strings.Contains(err.Error(), "390201") { // ErrObjectNotExistOrAuthorized; this way of testing error code is used in errors_test in the gosnowflake repo
				err = ErrObjectNotExistOrAuthorized
			}
			yield(DatabaseRole{}, err)
			return
		}
		defer rows.Close()
		for rows.Next() {
			var roleName semantics.Ident
			if err = rows.Scan(&roleName); err != nil {
				yield(DatabaseRole{}, err)
				return
			}
			if r, err := newDatabaseRoleFromString(synCnf, cnf, db, roleName); err != nil {
				yield(DatabaseRole{}, err)
				return
			} else {
				if !yield(r, nil) {
					return
				}
			}
		}
		if err = rows.Err(); err != nil {
			yield(DatabaseRole{}, err)
		}
	}
}

func GrantCreateDatabaseRoleToSelf(ctx context.Context, cnf *Config, conn *sql.DB, db semantics.Ident) error {
	return runSQL(ctx, cnf, conn, `GRANT CREATE DATABASE ROLE ON DATABASE IDENTIFIER(?) TO ROLE IDENTIFIER(?)`, db, cnf.Role)
}

func (r DatabaseRole) Create(ctx context.Context, cnf *Config, conn *sql.DB) error {
	return runSQL(ctx, cnf, conn, `CREATE DATABASE ROLE IF NOT EXISTS IDENTIFIER(?)`, r.String())
}

func (r DatabaseRole) hasUnmanagedPrivileges(ctx context.Context, cnf *Config, conn *sql.DB) (bool, error) {
	for _, err := range QueryGrantsToDBRoleFilteredLimit(ctx, cnf, conn, r.Database, r.Name, true, nil, cnf.DatabaseRolePrivileges[r.Mode], 1) {
		if err != nil {
			return true, err
		}
		return true, nil // there was an unmanaged grant, it does not matter what it was
	}
	return false, nil
}

func (r DatabaseRole) Drop(ctx context.Context, cnf *Config, conn *sql.DB) error {
	if has, err := r.hasUnmanagedPrivileges(ctx, cnf, conn); err != nil {
		return err
	} else if has {
		log.Printf("Database role %s has privileges not managed by Grupr, skipping dropping\n", r)
		return nil
	}
	// TODO: also check whether database role has been granted to roles or users other than grupr managed product roles,
	// and if so, refuse to drop, logging a line explaining the reason. Although, if the role has no unmanaged
	// privileges, it may not be harmful to drop it anyway.
	err := runSQL(ctx, cnf, conn, `DROP DATABASE ROLE IF EXISTS IDENTIFIER(?)`, r.String())
	if err == ErrObjectNotExistOrAuthorized {
		// if the DB does not exist anymore, then neither would the database role, and our job is done
		err = nil
	}
	return err
}

func (r DatabaseRole) String() string {
	return fmt.Sprintf("%v.%v", r.Database, r.Name)
}
