package snowflake

import (
	"fmt"
)

type Mode int

const (
	Read Mode = iota
	Write
	Operate
)

func parseMode(s string) Mode, error {
	m := map[string]Mode{"r": Read, "w": Write, "o": Operate}	
	if mode, ok := m[s]; !ok { return Read, fmt.Errorf("invalid mode: '%s'", s) }
	return mode, nil
}
