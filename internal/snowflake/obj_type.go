package snowflake

type ObjType int

const (
	ObjTpOther ObjType = iota // zero type
	ObjTpAccount
	ObjTpDatabase
	ObjTpDatabaseRole
	ObjTpDynamicTable
	ObjTpEventTable
	ObjTpExternalTable
	ObjTpHybridTable
	ObjTpIcebergTable
	ObjTpInteractiveTable
	ObjTpMaterializedView
	ObjTpOnlineFeatureTable
	ObjTpRole
	ObjTpSchema
	ObjTpSemanticView
	ObjTpTable
	ObjTpUser
	ObjTpView
	ObjTpWarehouse
)

func ParseObjType(s string) ObjType {
	// s is a statement-style object type string
	return map[string]ObjType{
		"ACCOUNT":              ObjTpAccount,
		"DATABASE":             ObjTpDatabase,
		"DATABASE ROLE":        ObjTpDatabaseRole,
		"DYNAMIC TABLE":        ObjTpDynamicTable,
		"EVENT TABLE":          ObjTpEventTable,
		"EXTERNAL TABLE":       ObjTpExternalTable,
		"HYBRID TABLE":         ObjTpHybridTable,
		"ICEBERG TABLE":        ObjTpIcebergTable,
		"INTERACTIVE TABLE":    ObjTpInteractiveTable,
		"MATERIALIZED VIEW":    ObjTpMaterializedView,
		"ONLINE FEATURE TABLE": ObjTpOnlineFeatureTable,
		"ROLE":                 ObjTpRole,
		"SCHEMA":               ObjTpSchema,
		"SEMANTIC VIEW":        ObjTpSemanticView,
		"TABLE":                ObjTpTable,
		"USER":                 ObjTpUser,
		"VIEW":                 ObjTpView,
		"WAREHOUSE":            ObjTpWarehouse,
	}[s]
}

func (ot ObjType) String() string {
	return map[ObjType]string{
		ObjTpOther:              "OTHER",
		ObjTpAccount:            "ACCOUNT",
		ObjTpDatabase:           "DATABASE",
		ObjTpDatabaseRole:       "DATABASE ROLE",
		ObjTpDynamicTable:       "DYNAMIC TABLE",
		ObjTpEventTable:         "EVENT TABLE",
		ObjTpExternalTable:      "EXTERNAL TABLE",
		ObjTpHybridTable:        "HYBRID TABLE",
		ObjTpIcebergTable:       "ICEBERG TABLE",
		ObjTpInteractiveTable:   "INTERACTIVE TABLE",
		ObjTpMaterializedView:   "MATERIALIZED VIEW",
		ObjTpOnlineFeatureTable: "ONLINE FEATURE TABLE",
		ObjTpRole:               "ROLE",
		ObjTpSchema:             "SCHEMA",
		ObjTpSemanticView:       "SEMANTIC VIEW",
		ObjTpTable:              "TABLE",
		ObjTpUser:               "USER",
		ObjTpView:               "VIEW",
		ObjTpWarehouse:          "WAREHOUSE",
	}[ot]
}

func (ot ObjType) RecordString() string {
	return ot.String() // TODO: and replace spaces with underscores
}

func ParseObjTypeFromRecord(s string) ObjType {
	// s is a record-style object type string as found in output of
	// SHOW OBJECTS and SHOW GRANTS
	return ParseObjType(s) // TODO: and first replace underscores with spaces
}

func (ot ObjType) getIdxObjectLevel() int {
	switch ot {
	case ObjTpTable:
		return 0
	case ObjTpView:
		return 1
	default:
		panic("not an object living within a schema or not yet implemented")
	}
}
