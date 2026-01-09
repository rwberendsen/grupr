package snowflake

import (
	"fmt"
)

type Mode int

const (
	ModeRead Mode = iota
	ModeWrite
	ModeOperate
)

func parseMode(s string) Mode, error {
	m := map[string]Mode{"r": ModeRead, "w": ModeWrite, "o": ModeOperate}	
	if mode, ok := m[s]; !ok { return Read, fmt.Errorf("invalid mode: '%s'", s) }
	return mode, nil
}

func (m Mode) String() string {
	return map[Mode]string{ModeRead: "r", ModeWrite: "w", ModeOperate: "o"}[m]
}
