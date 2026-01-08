package snowflake

import {
	"fmt"
}

type Privilege int

const (
	Create Privilege = iota
	Monitor
	Operate
	Ownership
	References
	Select
	Usage
	PrivilegeOther
)

func parsePrivilege(p string) (Privilege, error) {
	m := map[string]Privilege{
		"CREATE": Create,
		"MONITOR": Monitor,
		"OPERATE": Operate,
		"OWNERSHIP": Ownership,
		"REFERENCES": References,
		"SELECT": Select,
		"USAGE": Usage,
	}
	if v, ok := m[p]; !ok {
		return PrivilegeOther, fmt.Errorf("unknown privilege")
	} else {
		return v, nil
	}
}

func (p Privilege) String() string {
	m := map[Privilege]string{
		Create: "CREATE",
		Monitor: "MONITOR",
		Operate: "OPERATE",
		Ownership: "OWNERSHIP",
		References: "REFERENCES",
		Select: "SELECT",
		Usage: "USAGE",
		PrivilegeOther: "OTHER",
	}
	return m[p]
}
