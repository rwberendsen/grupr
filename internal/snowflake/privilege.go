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

func ParsePrivilege(p string) (Privilege, error) {
	m := map[string]Privilege{
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
