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

func ParseObjTypeFromShowObjectsRecord(kind string, is_hybrid bool, is_dynamic bool, is_iceberg bool,
	is_interactive bool) (ObjType, bool) {
	switch kind {
	case "TABLE":
		// TODO: there appears to be something like a dynamic iceberg table, for example, not sure how it would be
		// represented here, and what would be the set of privileges we can assign an object like that, it is not
		// separate treated in the Snowflake documentation page on access control privileges as of April 2026
		if is_hybrid && !is_dynamic && !is_iceberg && !is_interactive {
			return ObjTpHybridTable, true
		}
		if !is_hybrid && is_dynamic && !is_iceberg && !is_interactive {
			return ObjTpDynamicTable, true
		}
		if !is_hybrid && !is_dynamic && is_iceberg && !is_interactive {
			return ObjTpIcebergTable, true
		}
		if !is_hybrid && !is_dynamic && !is_iceberg && is_interactive {
			return ObjTpInteractiveTable, true
		}
	case "ONLINE_FEATURE_TABLE":
		return ObjTpOnlineFeatureTable, true
	case "EVENT_TABLE": // TODO: validate this is how it appears in the output
		return ObjTpEventTable, true
	case "EXTERNAL_TABLE": // TODO: validate this is how it appears in the output
		return ObjTpExternalTable, true
	case "VIEW":
		return ObjTpView, false // we cannot fully determine object type
	}
	return ObjTpOther, false
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
