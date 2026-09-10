package snowflake

import (
	"context"
	"database/sql"

	"github.com/snowflakedb/gosnowflake"
)

func GetDB(ctx context.Context, snowCnf *Config) (*sql.DB, error) {
	cnf := &gosnowflake.Config{
		Account:       snowCnf.Account,
		User:          string(snowCnf.User),
		Role:          string(snowCnf.Role),
		Database:      string(snowCnf.Database),
		Schema:        string(snowCnf.Schema),
		Warehouse:     string(snowCnf.Warehouse),
		Authenticator: gosnowflake.AuthTypeExternalBrowser,
	}
	if snowCnf.RSAKeyPath == "" {
		cnf.Authenticator = gosnowflake.AuthTypeExternalBrowser
	} else {
		if rsaKey, err := getPrivateRSAKey(snowCnf.RSAKeyPath); err != nil {
			return nil, err
		} else {
			cnf.Authenticator = gosnowflake.AuthTypeJwt
			cnf.PrivateKey = rsaKey
		}
	}
	var conn *sql.DB
	var err error
	if snowCnf.UseSQLOpen {
		var dsn string
		if dsn, err = gosnowflake.DSN(cnf); err == nil {
			conn, err = sql.Open("snowflake", dsn)
		}
	} else {
		connector := gosnowflake.NewConnector(gosnowflake.SnowflakeDriver{}, *cnf)
		conn = sql.OpenDB(connector)
		err = conn.PingContext(ctx)
	}
	if err != nil {
		return nil, err
	}
	conn.SetMaxOpenConns(snowCnf.MaxOpenConns)
	conn.SetMaxIdleConns(snowCnf.MaxIdleConns)
	return conn, nil
}
