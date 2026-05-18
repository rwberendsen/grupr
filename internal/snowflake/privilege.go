package snowflake

type Privilege int

const (
	PrvOther Privilege = iota // zero type is PrvOther
	PrvApplyBudget
	PrvCreate
	PrvCreateDatabaseRole
	PrvDelete
	PrvEvolveSchema
	PrvInsert
	PrvMonitor
	PrvOperate
	PrvOwnership
	PrvReferences
	PrvSelect
	PrvSelectErrorTable
	PrvTruncate
	PrvUpdate
	PrvUsage
)

func ParsePrivilege(p string) Privilege {
	return map[string]Privilege{
		"APPLYBUDGET":          PrvApplyBudget,
		"CREATE":               PrvCreate,
		"CREATE DATABASE ROLE": PrvCreateDatabaseRole,
		"DELETE":               PrvDelete,
		"EVOLVE SCHEMA":        PrvEvolveSchema,
		"INSERT":               PrvInsert,
		"MONITOR":              PrvMonitor,
		"OPERATE":              PrvOperate,
		"OWNERSHIP":            PrvOwnership,
		"REFERENCES":           PrvReferences,
		"SELECT":               PrvSelect,
		"SELECT ERROR TABLE":   PrvSelect,
		"TRUNCATE":             PrvTruncate,
		"UPDATE":               PrvUpdate,
		"USAGE":                PrvUsage,
	}[p]
}

func (p Privilege) String() string {
	return map[Privilege]string{
		PrvOther:              "OTHER",
		PrvApplyBudget:        "APPLYBUDGET",
		PrvCreate:             "CREATE",
		PrvCreateDatabaseRole: "CREATE DATABASE ROLE",
		PrvDelete:             "DELETE",
		PrvEvolveSchema:       "EVOLVE SCHEMA",
		PrvInsert:             "INSERT",
		PrvMonitor:            "MONITOR",
		PrvOperate:            "OPERATE",
		PrvOwnership:          "OWNERSHIP",
		PrvReferences:         "REFERENCES",
		PrvSelect:             "SELECT",
		PrvSelectErrorTable:   "SELECT ERROR TABLE",
		PrvTruncate:           "TRUNCATE",
		PrvUpdate:             "UPDATE",
		PrvUsage:              "USAGE",
	}[p]
}

func setFlagPrivilegeWarehouse(flags [3]bool, setFlag Privilege) [3]bool {
	switch setFlag {
	case PrvUsage:
		flags[0] = true
	case PrvMonitor:
		flags[1] = true
	case PrvOperate:
		flags[2] = true
	}
	return flags
}

func hasFlagPrivilegeWarehouse(flags [3]bool, flag Privilege) bool {
	switch flag {
	case PrvUsage:
		return flags[0]
	case PrvMonitor:
		return flags[1]
	case PrvOperate:
		return flags[2]
	}
	return false
}
