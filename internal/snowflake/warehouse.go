pacakge snowflake

import (
	"fmt"
)

type Warehouse struct {
        Ident         string   `yaml:"ident"`
	Mode          string   `yaml:"mode"`
        OnlyProd      bool     `yaml:"only_prod",omitempty"`
        OnlyNonProd   bool     `yaml:"only_non_prod",omitempty"`
        SharedBetween []string `yaml:"shared_between,omitempty"`
}
