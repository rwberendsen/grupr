package snowflake

import (
	"os"
	"strings"

	"github.com/rwberendsen/grupr/internal/semantics"
	"github.com/rwberendsen/grupr/internal/syntax"
)

type Config struct {
	User                    string
	Role                    string
	Account                 string
	Database                string
	Schema                  string
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
		MaxOpenConns:            0, // unlimited
		MaxIdleConns:            3, // MaxProductDTAPThreads - 1 (sometimes we use only one conn before quickly fanning out again)
		MaxProductDTAPThreads:   4,
		StmtBatchSize:           100,
		MaxProductDTAPRefreshes: 4,
		Modes:                   [1]Mode{Read},
		DryRun:                  true,
	}

	if user, ok := os.LookupEnv("GRUPR_SNOWFLAKE_USER"); !ok {
		return nil, fmt.Errorf("Could not find environment variable GRUPR_SNOWFLAKE_USER")
	} else {
		if !semCnf.ValidUnquotedExpr.MatchString(user) {
			return nil, fmt.Errorf("GRUPR_SNOWFLAKE_USER: Invalid user name")
		}
		cnf.User = strings.ToUpper(user)
	}

	if role, ok := os.LookupEnv("GRUPR_SNOWFLAKE_ROLE"); !ok {
		return nil, fmt.Errorf("Could not find environment variable GRUPR_SNOWFLAKE_USER")
	} else {
		if !semCnf.ValidUnquotedExpr.MatchString(role) {
			return nil, fmt.Errorf("GRUPR_SNOWFLAKE_ROLE: Invalid role name")
		}
		cnf.Role = strings.ToUpper(role)
	}

	if account, ok := os.LookupEnv("GRUPR_SNOWFLAKE_ACCOUNT"); !ok {
		return nil, fmt.Errorf("Could not find environment variable GRUPR_SNOWFLAKE_ACCOUNT")
	} else {
		cnf.Account = strings.ToUpper(account)
	}

	if database, ok := os.LookupEnv("GRUPR_SNOWFLAKE_DB"); !ok {
		return nil, fmt.Errorf("Could not find environment variable GRUPR_SNOWFLAKE_DB")
	} else {
		if !semCnf.ValidUnquotedExpr.MatchString(database) {
			return nil, fmt.Errorf("GRUPR_SNOWFLAKE_DB: Invalid database name")
		}
		cnf.Database = strings.ToUpper(database)
	}

	if schema, ok := os.LookupEnv("GRUPR_SNOWFLAKE_SCHEMA"); !ok {
		return nil, fmt.Errorf("Could not find environment variable GRUPR_SNOWFLAKE_SCHEMA")
	} else {
		if !semCnf.ValidUnquotedExpr.MatchString(schema) {
			return nil, fmt.Errorf("GRUPR_SNOWFLAKE_SCHEMA: Invalid schema name")
		}
		cnf.Schema = strings.ToUpper(schema)
	}

	if useSQLOpen, ok := os.LookupEnv("GRUPR_SNOWFLAKE_USE_SQL_OPEN"); ok {
		if b, err := strconv.ParseBool(useSQLOpen); err != nil {
			return fmt.Errorf("GRUPR_SNOWFLAKE_USE_SQL_OPEN: %w", err)
		}
		cnf.UseSQLOpen = b
	}

	if rsaKeyPath, ok := os.LookupEnv("GRUPR_SNOWFLAKE_RSA_KEY_PATH"); ok {
		cnf.RsaKeyPath = rsaKeyPath
	}

	if objectPrefix, ok := os.LookupEnv("GRUPR_SNOWFLAKE_OBJECT_PREFIX"); ok {
		if err := !syntax.validateID(objectPrefix); err != nil {
			return nil, fmt.Errorf("invalid value for GRUPR_SNOWFLAKE_OBJECT_PREFIX")
		}
		cnf.ObjectPrefix = strings.ToUpper(objectPrefix)
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
			if i < cnf.MaxOpenConnections {
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
			Privilege:     PrvUsage,
			GrantTemplate: ObjTpDatabase,
		}: {},
		GrantTemplate{
			Privilege: PrvUsage,
			GrantedOn: ObjTpSchema,
		}: {},
		GrantTemplate{
			Privilege: PrvSelect,
			GrantedOn: ObjTpTable,
		}: {},
		GrantTemplate{
			Privilege: PrvSelect,
			GrantedOn: ObjTpView,
		}: {},
		GrantTemplate{
			Privilege: PrvReferences,
			GrantedOn: ObjTpTable,
		}: {},
		GrantTemplate{
			Privilege: PrvReferences,
			GrantedOn: ObjTpView,
		}: {},
	}

	cnf.ProductRolePrivileges = map[Mode]map[GrantTemplate]struct{}{}
	cnf.ProductRolePrivileges[ModeRead] = map[GrantTemplate]struct{}{
		GrantTemplate{
			Privilege:                   PrvUsage,
			GrantedOn:                   ObjTpDatabaseRole,
			GrantedRoleStartsWithPrefix: newTrue(),
		}: {},
	}
}
