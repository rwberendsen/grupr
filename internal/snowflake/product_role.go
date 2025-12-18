package snowflake

import (
	"fmt"
	"strings"

	"github.com/rwberendsen/grupr/internal/syntax"
)

type ProductRole struct {
	ProductID string
	DTAP string
	Mode bool
	ID string
}

func newProductRole(synCnf *syntax.Config, cnf *Config, productID string, dtap string, mode string) ProductRole {
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

func (r ProductRole) String() string {
	return r.ID
}
