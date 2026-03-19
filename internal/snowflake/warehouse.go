package snowflake

import (
	"github.com/rwberendsen/grupr/internal/semantics"
)

type WarehouseDecoded struct {
        Ident         string   `yaml:"ident"`
	Mode          string   `yaml:"mode"`
        SharedBetween []string `yaml:"shared_between,omitempty"`
        OnlyProd      bool     `yaml:"only_prod",omitempty"`
        OnlyNonProd   bool     `yaml:"only_non_prod",omitempty"`
}

type Warehouse struct {
	Ident         semantics.Ident
	Mode	      Mode
	SharedBetween map[string]struct{}
	OnlyProd      bool
	OnlyNonProd   bool
}

// check:
// - When Mode == ModeWrite, then either OnlyProd or OnlyNonProd has to be set.
