package snowflake

import (
	"context"
	"crypto/rsa"
	"crypto/x509"
	"database/sql"
	"encoding/base64"
	"fmt"
	"log"

	"github.com/rwberendsen/grupr/internal/util"
	"github.com/snowflakedb/gosnowflake"
)

func GetDB(ctx context.Context, snowCnf *Config) (*sql.DB, error) {
	var conn *sql.DB
	var rsaKey *rsa.PrivateKey
	if snowCnf.UseSQLOpen {
		dsn := fmt.Sprintf("%v@%v/%v?authenticator=%s", util.EscapeQuotes(snowCnf.User.String()), snowCnf.Account,
			util.EscapeQuotes(snowCnf.Database.String()), gosnowflake.AuthTypeExternalBrowser.String())
		log.Printf("dsn: %v", dsn)
		var err error
		conn, err = sql.Open("snowflake", dsn)
		if err != nil {
			return nil, err
		}
	} else {
		var cnf *gosnowflake.Config
		if snowCnf.RSAKeyPath == "" {
			cnf = &gosnowflake.Config{
				Account:       snowCnf.Account,
				User:          string(snowCnf.User),
				Role:          string(snowCnf.Role),
				Database:      string(snowCnf.Database),
				Authenticator: gosnowflake.AuthTypeExternalBrowser,
				Params:        map[string]*string{},
			}
		} else {
			var err error
			rsaKey, err = getPrivateRSAKey(snowCnf.RSAKeyPath)
			if err != nil {
				return nil, err
			}
			cnf = &gosnowflake.Config{
				Account:       snowCnf.Account,
				User:          string(snowCnf.User),
				Role:          string(snowCnf.Role),
				Database:      string(snowCnf.Database),
				Authenticator: gosnowflake.AuthTypeJwt,
				PrivateKey:    rsaKey,
				Params:        map[string]*string{},
			}
		}
		connector := gosnowflake.NewConnector(gosnowflake.SnowflakeDriver{}, *cnf)
		conn = sql.OpenDB(connector)
	}
	conn.SetMaxOpenConns(snowCnf.MaxOpenConns)
	conn.SetMaxIdleConns(snowCnf.MaxIdleConns)
	err := conn.PingContext(ctx)
	if err != nil {
		if rsaKey != nil {
			log.Printf("please make sure public key is registered in Snowflake:")
			pubKeyByte, _ := x509.MarshalPKIXPublicKey(rsaKey.Public())
			log.Print(base64.StdEncoding.EncodeToString(pubKeyByte))
		}
		return conn, err
	}
	return conn, nil
}
