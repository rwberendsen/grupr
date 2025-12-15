package snowflake

import (
	"fmt"
	"strings"

	"github.com/rwberendsen/grupr/internal/syntax"
)

type ProductRole struct {
	DTAP string
	ProductID string
	ReadOnly bool
}

func newProductRole(synCnf *syntax.Config, cnf *Config, role string) (ProductRole, error) {
	pr := ProductRole{}
	if !role.HasPrefix(cnf.Prefix) { return pr, fmt.Errorf("Role does not start with Grupr prefix") }
	synCnf.Infix	
	// WIP: remove prefix and split around infix, parse dtap, product, and mode parts
}
