package snowflake

import (
	"fmt"
	"strings"

	"github.com/rwberendsen/grupr/internal/syntax"
)

type DatabaseRole struct {
	ProductID string
	DTAP string
	InterfaceID string // "" means this is a product-level database role
	Mode bool
	ID string
}

func newDatabaseRole(synCnf *syntax.Config, cnf *Config, productID string, dtap string, interfaceID string, mode string) DatabaseRole {
	r := DatabaseRole{
		ProductID: productID,
		DTAP: dtap,
		InterfaceID: interfaceID,
		Mode: mode,
	}
	if interfaceID == "" {
		r.ID = strings.ToUpper(synCnf.Prefix + productID + cnf.Infix + dtap + +cnf.Infix + mode)
	} else {
		r.ID = strings.ToUpper(synCnf.Prefix + productID + cnf.Infix + dtap + +cnf.Infix + interfaceID + cnf.Infix + mode)
	}
	return r
}

func newDatabaseRoleFromString(synCnf *syntax.Config, cnf *Config, role string) (DatabaseRole, error) {
	r := DatabaseRole{ID: role,}
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

func (r DatabaseRole) String() string {
	return r.ID
}
