package snowflake

import (
	"fmt"
	"maps"
	"os"
	"strconv"
	"strings"

	"github.com/rwberendsen/grupr/internal/semantics"
	"github.com/rwberendsen/grupr/internal/util"
)

type Config struct {
	User                      semantics.Ident
	Role                      semantics.Ident
	Account                   string
	Database                  semantics.Ident
	Schema                    semantics.Ident
	UseSQLOpen                bool
	RSAKeyPath                string
	MaxOpenConns              int
	MaxIdleConns              int
	MaxProductDTAPThreads     int
	StmtBatchSize             int
	MaxProductDTAPRefreshes   int
	Modes                     [1]Mode
	SystemDefinedRoles        []semantics.Ident
	ObjectPrivilegesRead      map[GrantTemplate]struct{}
	ObjectPrivilegesOwnership map[GrantTemplate]struct{}
	ObjectPrivilegesWrite     map[GrantTemplate]struct{}
	ObjectPrivileges          map[GrantTemplate]struct{}
	ManagedObjTypes           map[ObjType]bool
	DryRun                    bool
}

func GetConfig(semCnf *semantics.Config) (*Config, error) {
	cnf := &Config{
		UseSQLOpen:                false,
		MaxOpenConns:              0, // unlimited
		MaxIdleConns:              3, // MaxProductDTAPThreads - 1 (sometimes we use only one conn before quickly fanning out again)
		MaxProductDTAPThreads:     4,
		StmtBatchSize:             100,
		MaxProductDTAPRefreshes:   4,
		Modes:                     [1]Mode{ModeRead},
		SystemDefinedRoles: []semantics.Ident{
			semantics.Ident("GLOBALORGADMIN"),
			semantics.Ident("ORGADMIN"),
			semantics.Ident("ACCOUNTADMIN"),
			semantics.Ident("SYSADMIN"),
			semantics.Ident("PUBLIC"),
			semantics.Ident("SECURITYADMIN"),
			semantics.Ident("USERADMIN"),
		},
		// Later, we'll extend grupr with other object types, and when we do it and folks upgrade,
		// we don't want grupr to all of a sudden claim management of all these other object types.
		// So we build in a mechanism for people to explicity turn new object types on.
		//
		// Note that ObjTpTable implies we also manage ObjTpHybridTable; in the output of SHOW GRANTS,
		// and in SQL statements like CREATE <object_type> the two are not distinguished
		ManagedObjTypes: map[ObjType]bool{
			ObjTpTable: true,
			ObjTpView: true,
		},
		DryRun: true,
	}

	if user, ok := os.LookupEnv("GRUPR_SNOWFLAKE_USER"); !ok {
		return nil, fmt.Errorf("Could not find environment variable GRUPR_SNOWFLAKE_USER")
	} else {
		if user, err := semantics.NewIdentStripQuotesIfAny(user, semCnf.ValidQuotedExpr, semCnf.ValidUnquotedExpr); err != nil {
			return nil, fmt.Errorf("GRUPR_SNOWFLAKE_USER: Invalid user name")
		} else {
			cnf.User = user
		}
	}

	if role, ok := os.LookupEnv("GRUPR_SNOWFLAKE_ROLE"); !ok {
		return nil, fmt.Errorf("Could not find environment variable GRUPR_SNOWFLAKE_USER")
	} else {
		if role, err := semantics.NewIdentStripQuotesIfAny(role, semCnf.ValidQuotedExpr, semCnf.ValidUnquotedExpr); err != nil {
			return nil, fmt.Errorf("GRUPR_SNOWFLAKE_ROLE: Invalid role name")
		} else {
			cnf.Role = role
		}
	}

	if account, ok := os.LookupEnv("GRUPR_SNOWFLAKE_ACCOUNT"); !ok {
		return nil, fmt.Errorf("Could not find environment variable GRUPR_SNOWFLAKE_ACCOUNT")
	} else {
		cnf.Account = strings.ToUpper(account)
	}

	if database, ok := os.LookupEnv("GRUPR_SNOWFLAKE_DB"); !ok {
		return nil, fmt.Errorf("Could not find environment variable GRUPR_SNOWFLAKE_DB")
	} else {
		if database, err := semantics.NewIdentStripQuotesIfAny(database, semCnf.ValidQuotedExpr, semCnf.ValidUnquotedExpr); err != nil {
			return nil, fmt.Errorf("GRUPR_SNOWFLAKE_DB: Invalid database name")
		} else {
			cnf.Database = database
		}
	}

	if schema, ok := os.LookupEnv("GRUPR_SNOWFLAKE_SCHEMA"); !ok {
		return nil, fmt.Errorf("Could not find environment variable GRUPR_SNOWFLAKE_SCHEMA")
	} else {
		if schema, err := semantics.NewIdentStripQuotesIfAny(schema, semCnf.ValidQuotedExpr, semCnf.ValidUnquotedExpr); err != nil {
			return nil, fmt.Errorf("GRUPR_SNOWFLAKE_SCHEMA: Invalid schema name")
		} else {
			cnf.Schema = schema
		}
	}

	if useSQLOpen, ok := os.LookupEnv("GRUPR_SNOWFLAKE_USE_SQL_OPEN"); ok {
		if b, err := strconv.ParseBool(useSQLOpen); err != nil {
			return nil, fmt.Errorf("GRUPR_SNOWFLAKE_USE_SQL_OPEN: %w", err)
		} else {
			cnf.UseSQLOpen = b
		}
	}

	if rsaKeyPath, ok := os.LookupEnv("GRUPR_SNOWFLAKE_RSA_KEY_PATH"); ok {
		cnf.RSAKeyPath = rsaKeyPath
	}

	if maxOpenConns, ok := os.LookupEnv("GRUPR_SNOWFLAKE_MAX_OPEN_CONNS"); ok {
		if i, err := strconv.Atoi(maxOpenConns); err != nil {
			return nil, fmt.Errorf("GRUPR_SNOWFLAKE_MAX_OPEN_CONNS: %w", err)
		} else {
			cnf.MaxOpenConns = i
		}
	}

	if maxIdleConns, ok := os.LookupEnv("GRUPR_SNOWFLAKE_MAX_IDLE_CONNS"); ok {
		if i, err := strconv.Atoi(maxIdleConns); err != nil {
			return nil, fmt.Errorf("GRUPR_SNOWFLAKE_MAX_IDLE_CONNS: %w", err)
		} else {
			cnf.MaxIdleConns = i
		}
	}

	if maxProductThreads, ok := os.LookupEnv("GRUPR_SNOWFLAKE_MAX_PRODUCT_DTAP_THREADS"); ok {
		if i, err := strconv.Atoi(maxProductThreads); err != nil {
			return nil, fmt.Errorf("GRUPR_SNOWFLAKE_MAX_PRODUCT_DTAP_THREADS: %w", err)
		} else {
			if i < cnf.MaxOpenConns {
				return nil, fmt.Errorf("GRUPR_SNOWFLAKE_MAX_PRODUCT_DTAP_THREADS should be >= GRUPR_SNOWFLAKE_MAX_OPEN_CONNECTIONS")
			}
			cnf.MaxProductDTAPThreads = i
		}
	}

	if stmtBatchSize, ok := os.LookupEnv("GRUPR_SNOWFLAKE_STMT_BATCH_SIZE"); ok {
		if i, err := strconv.Atoi(stmtBatchSize); err != nil {
			return nil, fmt.Errorf("GRUPR_SNOWFLAKE_STMT_BATCH_SIZE: %w", err)
		} else {
			cnf.StmtBatchSize = i
		}
	}

	if maxProductRefreshes, ok := os.LookupEnv("GRUPR_SNOWFLAKE_MAX_PRODUCT_DTAP_REFRESHES"); ok {
		if i, err := strconv.Atoi(maxProductRefreshes); err != nil {
			return nil, fmt.Errorf("GRUPR_SNOWFLAKE_MAX_PRODUCT_DTAP_REFRESHES: %w", err)
		} else {
			cnf.MaxProductDTAPRefreshes = i
		}
	}

	for ot := range []ObjType{ObjTpMaterializedView} {
		envStr := fmt.Sprintf("GRUPR_SNOWFLAKE_MA_%v", ot)
		if env, ok := os.LookupEnv(envStr); ok {
			if b, err := strconv.ParseBool(env); err != nil {
				return nil, fmt.Errorf("%s: %w", envStr, err)
			} else {
				cnf.ManagedObjTypes[ot] = b
			}
		}
	}

	// These are the object privileges that are managed for read database roles
	cnf.ObjectPrivilegesRead = map[GrantTemplate]struct{}{
		GrantTemplate{
			PrivilegeComplete: PrivilegeComplete{Privilege: PrvUsage},
			GrantedOn:         ObjTpDatabase,
		}: {},
		GrantTemplate{
			PrivilegeComplete: PrivilegeComplete{Privilege: PrvMonitor},
			GrantedOn:         ObjTpDatabase,
		}: {},
		GrantTemplate{
			PrivilegeComplete: PrivilegeComplete{Privilege: PrvUsage},
			GrantedOn:         ObjTpSchema,
		}: {},
		GrantTemplate{
			PrivilegeComplete: PrivilegeComplete{Privilege: PrvMonitor},
			GrantedOn:         ObjTpSchema,
		}: {},
		GrantTemplate{
			PrivilegeComplete: PrivilegeComplete{Privilege: PrvSelect},
			GrantedOn:         ObjTpTable,
		}: {},
		GrantTemplate{
			PrivilegeComplete: PrivilegeComplete{Privilege: PrvSelectErrorTable},
			GrantedOn:         ObjTpTable,
		}: {},
		GrantTemplate{
			PrivilegeComplete: PrivilegeComplete{Privilege: PrvReferences},
			GrantedOn:         ObjTpTable,
		}: {},
		GrantTemplate{
			PrivilegeComplete: PrivilegeComplete{Privilege: PrvSelect},
			GrantedOn:         ObjTpView,
		}: {},
		GrantTemplate{
			PrivilegeComplete: PrivilegeComplete{Privilege: PrvReferences},
			GrantedOn:         ObjTpView,
		}: {},
	}
	if cnf.ManagedObjTypes[ObjTpMaterializedView] {
		cnf.ObjectPrivilegesRead[GrantTemplate{
				PrivilegeComplete: PrivilegeComplete{Privilege: PrvSelect},
				GrantedOn:         ObjTpMaterializedView,
		}] = {}
		cnf.ObjectPrivilegesRead[GrantTemplate{
			PrivilegeComplete: PrivilegeComplete{Privilege: PrvReferences},
			GrantedOn:         ObjTpMaterializedView,
		}] = {}
	}

	// These are the object privileges that are managed for product write roles
	cnf.ObjectPrivilegesOwnership = map[GrantTemplate]struct{}{
		GrantTemplate{
			PrivilegeComplete: PrivilegeComplete{Privilege: PrvCreate, CreateObjectType: ObjTpTable},
			GrantedOn:         ObjTpSchema,
		}: {},
		GrantTemplate{
			PrivilegeComplete: PrivilegeComplete{Privilege: PrvCreate, CreateObjectType: ObjTpView},
			GrantedOn:         ObjTpSchema,
		}: {},
		GrantTemplate{
			PrivilegeComplete: PrivilegeComplete{Privilege: PrvOwnership},
			GrantedOn:         ObjTpTable,
		}: {},
		GrantTemplate{
			PrivilegeComplete: PrivilegeComplete{Privilege: PrvOwnership},
			GrantedOn:         ObjTpView,
		}: {},
	}
	if cnf.ManagedObjTypes[ObjTpMaterializedView] {
		cnf.ObjectPrivilegesOwnership[GrantTemplate{
			PrivilegeComplete: PrivilegeComplete{Privilege: PrvCreate, CreateObjectType: ObjTpMaterializedView},
			GrantedOn:         ObjTpSchema,
		}] = {}
		cnf.ObjectPrivilegesOwnership[GrantTemplate{
			PrivilegeComplete: PrivilegeComplete{Privilege: PrvOwnership},
			GrantedOn:         ObjTpMaterializedView,
		}] = {}
	}

	// We need the next one to manage access exclusively,
	// when we will revoke such privileges from any user managed or even system roles
	cnf.ObjectPrivilegesWrite = map[GrantTemplate]struct{}{
		GrantTemplate{
			PrivilegeComplete: PrivilegeComplete{Privilege: PrvInsert},
			GrantedOn:         ObjTpTable,
		}: {},
		GrantTemplate{
			PrivilegeComplete: PrivilegeComplete{Privilege: PrvUpdate},
			GrantedOn:         ObjTpTable,
		}: {},
		GrantTemplate{
			PrivilegeComplete: PrivilegeComplete{Privilege: PrvTruncate},
			GrantedOn:         ObjTpTable,
		}: {},
		GrantTemplate{
			PrivilegeComplete: PrivilegeComplete{Privilege: PrvDelete},
			GrantedOn:         ObjTpTable,
		}: {},
		GrantTemplate{
			PrivilegeComplete: PrivilegeComplete{Privilege: PrvEvolveSchema},
			GrantedOn:         ObjTpTable,
		}: {},
		GrantTemplate{
			PrivilegeComplete: PrivilegeComplete{Privilege: PrvApplyBudget},
			GrantedOn:         ObjTpTable,
		}: {},
	}
	if cnf.ManagedObjTypes[ObjTpMaterializedView] {
		cnf.ObjectPrivilegesWrite[GrantTemplate{
			PrivilegeComplete: PrivilegeComplete{Privilege: PrvApplyBudget},
			GrantedOn:         ObjTpMaterializedView,
		}] = {}
	}

	// This one, too, will be used for managing access exclusively
	cnf.ObjectPrivileges = map[GrantTemplate]struct{}{}
	maps.Copy(cnf.ObjectPrivileges, cnf.ObjectPrivilegesRead)
	maps.Copy(cnf.ObjectPrivileges, cnf.ObjectPrivilegesWrite)
	maps.Copy(cnf.ObjectPrivileges, cnf.ObjectPrivilegesOwnership)

	// These are compute privileges that are managed for product roles
	cnf.ComputePrivileges = map[GrantTemplate]struct{}{
		GrantTemplate{
			PrivilegeComplete: PrivilegeComplete{Privilege: PrvUsage},
			GrantedOn:         ObjTpWarehouse,
		}: {},
		GrantTemplate{
			PrivilegeComplete: PrivilegeComplete{Privilege: PrvMonitor},
			GrantedOn:         ObjTpWarehouse,
		}: {},
		GrantTemplate{
			PrivilegeComplete: PrivilegeComplete{Privilege: PrvOperate},
			GrantedOn:         ObjTpWarehouse,
		}: {},
	}

	// These are database role usage privileges which are managed for product roles
	// The addition that the DB role should be grupr managed means that granting
	// a non-grupr-managed DB role to a grupr managed product role is perfectly fine,
	// it will be considered an unmanaged grant
	cnf.DBRolePrivileges = map[GrantTemplate]struct{}{
		GrantTemplate{
			PrivilegeComplete:         PrivilegeComplete{Privilege: PrvUsage},
			GrantedOn:                 ObjTpDatabaseRole,
			// Note that it's a bit odd that if we created another entry just like this, because it's a different
			// pointer, it would be a different key in the map.
			// TODO: use slices instead of maps for cnf.DBROlePrivileges and friends
			GrantedRoleIsGruprManaged: util.NewTrue(), 
		}: {},
	}


	if dryRun, ok := os.LookupEnv("GRUPR_SNOWFLAKE_DRY_RUN"); ok {
		if b, err := strconv.ParseBool(dryRun); err != nil {
			return nil, fmt.Errorf("GRUPR_SNOWFLAKE_DRY_RUN: %w", err)
		} else {
			cnf.DryRun = b
		}
	}

	return cnf, nil
}
