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

func ParseMode(s string) (Mode, error) {
	m := map[string]Mode{"r": ModeRead, "w": ModeWrite, "o": ModeOperate}
	if mode, ok := m[s]; !ok {
		return ModeRead, fmt.Errorf("invalid mode: '%s'", s)
	} else {
		return mode, nil
	}
}

func (m Mode) String() string {
	return map[Mode]string{ModeRead: "r", ModeWrite: "w", ModeOperate: "o"}[m]
}

func (m Mode) getIdx() int {
	switch m {
	case ModeRead:
		return 0
	case ModeWrite:
		return 1
	default:
		panic("we don't currently use arrays with more modes")
	}
}

func setFlagMode(flags [2]bool, setFlag Mode) [2]bool {
	flags[setFlag.getIdx()] = true
	return flags
}
