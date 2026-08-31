package snowflake

import (
	"fmt"
	"strings"

	"github.com/rwberendsen/grupr/internal/semantics"
)

type ObjType int

const (
	ObjTpOther ObjType = iota // zero type
	ObjTpAccount
	ObjTpDatabase
	ObjTpDatabaseRole
	ObjTpHybridTable
	ObjTpMaterializedView
	ObjTpRole
	ObjTpSchema
	ObjTpTable
	ObjTpUser
	ObjTpView
	ObjTpWarehouse
)

func ParseObjType(s string) ObjType {
	// s is a statement-style object type string
	// Note that even though hybrid tables are managed by grupr, they
	// are just called TABLE in many SQL statements, and they also
	// appear as TABLE in the output of SHOW GRANTS commands.
	//
	// Afaik, currently hybrid tables are the only special table type
	// that cannot be distinguished from regular tables in SQL statements
	// and the output of SHOW GRANTS statements.
	return map[string]ObjType{
		"ACCOUNT":           ObjTpAccount,
		"DATABASE":          ObjTpDatabase,
		"DATABASE ROLE":     ObjTpDatabaseRole,
		"MATERIALIZED VIEW": ObjTpMaterializedView,
		"ROLE":              ObjTpRole,
		"SCHEMA":            ObjTpSchema,
		"TABLE":             ObjTpTable,
		"USER":              ObjTpUser,
		"VIEW":              ObjTpView,
		"WAREHOUSE":         ObjTpWarehouse,
	}[s]
}

func (ot ObjType) String() string {
	// String representation of object type ready to be used
	// in SQL statements
	return map[ObjType]string{
		ObjTpOther:            "OTHER",
		ObjTpAccount:          "ACCOUNT",
		ObjTpDatabase:         "DATABASE",
		ObjTpDatabaseRole:     "DATABASE ROLE",
		ObjTpHybridTable:      "TABLE", // Hybrid tables are indistinguishable from regular table in many SQL statements
		ObjTpMaterializedView: "MATERIALIZED VIEW",
		ObjTpRole:             "ROLE",
		ObjTpSchema:           "SCHEMA",
		ObjTpTable:            "TABLE",
		ObjTpUser:             "USER",
		ObjTpView:             "VIEW",
		ObjTpWarehouse:        "WAREHOUSE",
	}[ot]
}

func (ot ObjType) RecordString() string {
	return strings.ReplaceAll(ot.String(), " ", "_")
}

func ParseObjTypeFromRecord(s string) ObjType {
	// s is a record-style object type string as found in output of
	// SHOW OBJECTS and SHOW GRANTS
	return ParseObjType(strings.ReplaceAll(s, "_", " "))
}

func (ot ObjType) FQN(db semantics.Ident, schema semantics.Ident, obj semantics.Ident) (fqn string) {
	switch ot {
	case ObjTpDatabase:
		if string(db) == "" {
			panic("db: empty string")
		}
		fqn = fmt.Sprintf("%s", db)
	case ObjTpDatabaseRole:
		if string(db) == "" {
			panic("db: empty string")
		}
		if string(obj) == "" {
			panic("obj: empty string")
		}
		fqn = fmt.Sprintf("%s.%s", db, obj)
	case ObjTpRole, ObjTpUser, ObjTpWarehouse:
		if string(obj) == "" {
			panic("obj: empty string")
		}
		fqn = fmt.Sprintf("%s", obj)
	case ObjTpSchema:
		if string(db) == "" {
			panic("db: empty string")
		}
		if string(schema) == "" {
			panic("schema: empty string")
		}
		fqn = fmt.Sprintf("%s.%s", db, schema)
	case ObjTpHybridTable, ObjTpMaterializedView, ObjTpTable, ObjTpView:
		if string(db) == "" {
			panic("db: empty string")
		}
		if string(schema) == "" {
			panic("schema: empty string")
		}
		if string(obj) == "" {
			panic("obj: empty string")
		}
		fqn = fmt.Sprintf("%s.%s.%s", db, schema, obj)
	default:
		panic(fmt.Sprintf("'%v': unsupported ObjType", ot))
	}
	return
}
