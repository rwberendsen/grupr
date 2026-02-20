package snowflake

import (
	"fmt"
	"os"
	"strconv"
	"strings"

	"github.com/rwberendsen/grupr/internal/semantics"
	"github.com/rwberendsen/grupr/internal/syntax"
	"github.com/rwberendsen/grupr/internal/util"
)

type Config struct {
	User                    semantics.Ident
	Role                    semantics.Ident
	Account                 string
	Database                semantics.Ident
	Schema                  semantics.Ident
	UseSQLOpen              bool
	RSAKeyPath              string
	ObjectPrefix            string // for objects (roles) created by Grupr in Snowflake
	MaxOpenConns            int
	MaxIdleConns            int
	MaxProductDTAPThreads   int
	StmtBatchSize           int
	MaxProductDTAPRefreshes int
	Modes                   [1]Mode
	DatabaseRolePrivileges  map[Mode]map[GrantTemplate]struct{}
	ProductRolePrivileges   map[Mode]map[GrantTemplate]struct{}
	DryRun                  bool
}

func GetConfig(semCnf *semantics.Config) (*Config, error) {
	cnf := &Config{
		UseSQLOpen:              false,
		ObjectPrefix:            "_x_",
		MaxOpenConns:            0, // unlimited
		MaxIdleConns:            3, // MaxProductDTAPThreads - 1 (sometimes we use only one conn before quickly fanning out again)
		MaxProductDTAPThreads:   4,
		StmtBatchSize:           100,
		MaxProductDTAPRefreshes: 4,
		Modes:                   [1]Mode{ModeRead},
		DryRun:                  true,
	}

	if user, ok := os.LookupEnv("GRUPR_SNOWFLAKE_USER"); !ok {
		return nil, fmt.Errorf("Could not find environment variable GRUPR_SNOWFLAKE_USER")
	} else {
		if user, err := semantics.NewIdentStripQuotesIfAny(semCnf, user); err != nil {
			return nil, fmt.Errorf("GRUPR_SNOWFLAKE_USER: Invalid user name")
		} else {
			cnf.User = user
		}
	}

	if role, ok := os.LookupEnv("GRUPR_SNOWFLAKE_ROLE"); !ok {
		return nil, fmt.Errorf("Could not find environment variable GRUPR_SNOWFLAKE_USER")
	} else {
		if role, err := semantics.NewIdentStripQuotesIfAny(semCnf, role); err != nil {
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
		if database, err := semantics.NewIdentStripQuotesIfAny(semCnf, database); err != nil {
			return nil, fmt.Errorf("GRUPR_SNOWFLAKE_DB: Invalid database name")
		} else {
			cnf.Database = database
		}
	}

	if schema, ok := os.LookupEnv("GRUPR_SNOWFLAKE_SCHEMA"); !ok {
		return nil, fmt.Errorf("Could not find environment variable GRUPR_SNOWFLAKE_SCHEMA")
	} else {
		if schema, err := semantics.NewIdentStripQuotesIfAny(semCnf, schema); err != nil {
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

	if objectPrefix, ok := os.LookupEnv("GRUPR_SNOWFLAKE_OBJECT_PREFIX"); ok {
		if err := syntax.ValidateID(objectPrefix); err != nil {
			return nil, fmt.Errorf("invalid value for GRUPR_SNOWFLAKE_OBJECT_PREFIX")
		}
		cnf.ObjectPrefix = strings.ToLower(objectPrefix)
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

	cnf.DatabaseRolePrivileges = map[Mode]map[GrantTemplate]struct{}{}
	cnf.DatabaseRolePrivileges[ModeRead] = map[GrantTemplate]struct{}{
		GrantTemplate{
			PrivilegeComplete: PrivilegeComplete{Privilege: PrvUsage},
			GrantedOn:         ObjTpDatabase,
		}: {},
		GrantTemplate{
			PrivilegeComplete: PrivilegeComplete{Privilege: PrvUsage},
			GrantedOn:         ObjTpSchema,
		}: {},
		GrantTemplate{
			PrivilegeComplete: PrivilegeComplete{Privilege: PrvSelect},
			GrantedOn:         ObjTpTable,
		}: {},
		GrantTemplate{
			PrivilegeComplete: PrivilegeComplete{Privilege: PrvSelect},
			GrantedOn:         ObjTpView,
		}: {},
		GrantTemplate{
			PrivilegeComplete: PrivilegeComplete{Privilege: PrvReferences},
			GrantedOn:         ObjTpTable,
		}: {},
		GrantTemplate{
			PrivilegeComplete: PrivilegeComplete{Privilege: PrvReferences},
			GrantedOn:         ObjTpView,
		}: {},
	}

	cnf.ProductRolePrivileges = map[Mode]map[GrantTemplate]struct{}{}
	cnf.ProductRolePrivileges[ModeRead] = map[GrantTemplate]struct{}{
		GrantTemplate{
			PrivilegeComplete:           PrivilegeComplete{Privilege: PrvUsage},
			GrantedOn:                   ObjTpDatabaseRole,
			GrantedRoleStartsWithPrefix: util.NewTrue(),
		}: {},
	}

	return cnf, nil
}
