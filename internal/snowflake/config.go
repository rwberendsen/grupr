package snowflake

import (
	"os"
	
	"github.com/rwberendsen/grupr/internal/syntax"
	"github.com/rwberendsen/grupr/internal/semantics"
)

type Config struct {
	User string
	Role string
	Account string
	Database string
	Schema string
	UseSQLOpen bool
	RSAKeyPath string
	ObjectPrefix string // for objects created by Grupr in Snowflake
	MaxOpenConns int
	MaxIdleConns int
	MaxProductThreads int
	MaxProductRefreshes int
	Modes [1]Mode
	DatabaseRolePrivileges map[Mode]map[GrantToRole]struct{}
	DryRun bool
}

func GetConfig(semCnf *semantics.Config) *Config, error {
	cnf := &Config{
		UseSQLOpen: false,
		MaxOpenConns: 0, 	// unlimited
		MaxIdleConns: 3,	// MaxProductThreads - 1 (sometimes we use only one conn before quickly fanning out again)
		MaxProductThreads: 4,
		MaxProductRefreshes: 4, 
		Modes: [1]Mode{Read,},
		DryRun: true
	}

	if user, ok := os.LookupEnv("GRUPR_SNOWFLAKE_USER"); !ok {
		return nil, fmt.Errorf("Could not find environment variable GRUPR_SNOWFLAKE_USER")
	} else {
		if !semCnf.ValidUnquotedExpr.MatchString(user) {
			return nil, fmt.Errorf("GRUPR_SNOWFLAKE_USER: Invalid user name")	
		}
		cnf.User = user
	}

	if role, ok := os.LookupEnv("GRUPR_SNOWFLAKE_ROLE"); !ok {
q		return nil, fmt.Errorf("Could not find environment variable GRUPR_SNOWFLAKE_USER")
	} else {
		if !semCnf.ValidUnquotedExpr.MatchString(role) {
			return nil, fmt.Errorf("GRUPR_SNOWFLAKE_ROLE: Invalid role name")	
		}
		cnf.Role = role
	}

	if account, ok := os.LookupEnv("GRUPR_SNOWFLAKE_ACCOUNT"); !ok {
		return nil, fmt.Errorf("Could not find environment variable GRUPR_SNOWFLAKE_ACCOUNT")
	} else {
		cnf.Account = account
	}

	if database, ok := os.LookupEnv("GRUPR_SNOWFLAKE_DB"); !ok {
		return nil, fmt.Errorf("Could not find environment variable GRUPR_SNOWFLAKE_DB")
	} else {
		cnf.Database = database
	}

	if useSQLOpen, ok := os.LookupEnv("GRUPR_SNOWFLAKE_USE_SQL_OPEN") {
		if b, err := strconv.ParseBool(useSQLOpen); err!= nil {
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
		cnf.ObjectPrefix = objectPrefix
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

	if maxProductThreads, ok := os.LookupEnv("GRUPR_SNOWFLAKE_MAX_PRODUCT_THREADS"); ok {
		if i, err := strconv.Atoi(maxProductThreads); err != nil {
			return nil, fmt.Errorf("GRUPR_SNOWFLAKE_MAX_PRODUCT_THREADS: %w", err)
		} else {
			if i < cnf.MaxOpenConnections {
				return nil, fmt.Errorf("GRUPR_SNOWFLAKE_MAX_PRODUCT_THREADS should be >= GRUPR_SNOWFLAKE_MAX_OPEN_CONNECTIONS")
			}
			cnf.MaxProductThreads = i
		}
	}
	
	if maxProductRefreshes, ok := os.LookupEnv("GRUPR_SNOWFLAKE_MAX_PRODUCT_REFRESHES"); ok {
		if i, err := strconv.Atoi(maxProductRefreshes); err != nil {
			return nil, fmt.Errorf("GRUPR_SNOWFLAKE_MAX_PRODUCT_REFRESHES: %w", err)
		} else {
			cnf.MaxProductRefreshes = i
		}
	}

	cnf.DatabaseRolePrivileges = map[Mode]map[GrantToRole]struct{}{}
	cnf.DatabaseRolePrivileges[ModeRead] = map[GrantToRole]struct{}{
		GrantToRole{
			Privilege: PrvUsage,
			GrantedOn: ObjTpDatabase,
		}: {},
		GrantToRole{
			Privilege: PrvUsage,
			GrantedOn: ObjTpSchema,
		}: {},
		GrantToRole{
			Privilege: PrvSelect,
			GrantedOn: ObjTpTable,
		}: {},
		GrantToRole{
			Privilege: PrvSelect,
			GrantedOn: ObjTpView,
		}: {},
		GrantToRole{
			Privilege: PrvReferences,
			GrantedOn: ObjTpTable,
		}: {},
		GrantToRole{
			Privilege: PrvReferences,
			GrantedOn: ObjTpView,
		}: {},
	}
}
