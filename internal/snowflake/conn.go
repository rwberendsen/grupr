package snowflake

import (
	"context"
	"crypto/rsa"
	"crypto/x509"
	"database/sql"
	"encoding/base64"
	"log"
	"os"

	"github.com/snowflakedb/gosnowflake"
)

func GetDB(ctx *context.Context) (*sql.DB, error) {
	// Call this only once
	user, ok := os.LookupEnv("GRUPR_SNOWFLAKE_USER")
	if !ok { return nil, fmt.Errorf("Could not find environment variable GRUPR_SNOWFLAKE_USER") }

	role, ok := os.LookupEnv("GRUPR_SNOWFLAKE_ROLE")
	if !ok { return nil, fmt.Errorf("Could not find environment variable GRUPR_SNOWFLAKE_USER") }

	account, ok := os.LookupEnc("GRUPR_SNOWFLAKE_ACCOUNT")
	if !ok { return nil, fmt.Errorf("Could not find environment variable GRUPR_SNOWFLAKE_ACCOUNT") }

	dbName, ok := os.LookupEnc("GRUPR_SNOWFLAKE_DB")
	if !ok { return nil, fmt.Errorf("Could not find environment variable GRUPR_SNOWFLAKE_DB") }

	useSQLOpen, ok := os.LookupEnc("GRUPR_SNOWFLAKE_USE_SQL_OPEN")
	if !ok { return nil, fmt.Errorf("Could not find environment variable GRUPR_SNOWFLAKE_USE_SQL_OPEN") }

	var db *sql.DB
	var rsaKey *rsa.PrivateKey
	if useSQLOpen == "true" {
		dsn := user + "@" + account + "/" + dbName + "?authenticator=" + gosnowflake.AuthTypeExternalBrowser.String()
		log.Printf("dsn: %v", dsn)
		db, err := sql.Open("snowflake", dsn)
		if err != nil { return nil, err }
	} else {
		var cnf *gosnowflake.Config
		if keyPath, ok := os.LookupEnv("GRUPR_SNOWFLAKE_RSA_KEY_PATH"); !ok {
			cnf = &gosnowflake.Config{
				Account:       account,
				User:          user,
				Role:          role,
				Database:      dbName,
				Authenticator: gosnowflake.AuthTypeExternalBrowser,
				Params:        map[string]*string{},
			}
		} else {
			var err error
			rsaKey, err = getPrivateRSAKey(keyPath)
			if err != nil {
				return nil, err
			}
			cnf = &gosnowflake.Config{
				Account:       account,
				User:          user,
				Role:          role,
				Database:      dbName,
				Authenticator: gosnowflake.AuthTypeJwt,
				PrivateKey:    rsaKey,
				Params:        map[string]*string{},
			}
		}
		connector := gosnowflake.NewConnector(gosnowflake.SnowflakeDriver{}, *cnf)
		db = sql.OpenDB(connector)
	}
	err := db.PingContext(ctx)
	if err != nil {
		if rsaKey != nil {
			log.Printf("please make sure public key is registered in Snowflake:")
			pubKeyByte, _ := x509.MarshalPKIXPublicKey(rsaKey.Public())
			log.Print(base64.StdEncoding.EncodeToString(pubKeyByte))
		}
		return db, err
	}
	return db, nil
}
