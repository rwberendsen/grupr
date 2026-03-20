package snowflake

type Privilege int

const (
	PrvOther Privilege = iota // zero type is PrvOther
	PrvCreate
	PrvMonitor
	PrvOperate
	PrvOwnership
	PrvReferences
	PrvSelect
	PrvUsage
)

func ParsePrivilege(p string) Privilege {
	return map[string]Privilege{
		"CREATE":     PrvCreate,
		"MONITOR":    PrvMonitor,
		"OPERATE":    PrvOperate,
		"OWNERSHIP":  PrvOwnership,
		"REFERENCES": PrvReferences,
		"SELECT":     PrvSelect,
		"USAGE":      PrvUsage,
	}[p]
}

func (p Privilege) String() string {
	return map[Privilege]string{
		PrvOther:      "OTHER",
		PrvCreate:     "CREATE",
		PrvMonitor:    "MONITOR",
		PrvOperate:    "OPERATE",
		PrvOwnership:  "OWNERSHIP",
		PrvReferences: "REFERENCES",
		PrvSelect:     "SELECT",
		PrvUsage:      "USAGE",
	}[p]
}

func (p Privilege) getIdxObjectLevel() int {
	switch p {
	case PrvSelect:
		return 0
	case PrvReferences:
		return 1
	default:
		panic("not an object level privilege or not yet implemented")
	}
}

func setFlagPrivilegeWarehouse(flags [2]bool, setFlag Privilege) [2]bool {
	switch setFlag {
	case PrvUsage:
		flags[0] = true
	case PrvOperate:
		flags[1] = true
	}
	return flags
}

func hasFlagPrivilegeWarehouse(flags [2]bool, flag Privilege) bool {
	switch flag {
	case PrvUsage:
		return flags[0]
	case PrvOperate:
		return flags[1]
	}
	return false
}
