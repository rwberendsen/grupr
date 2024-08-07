package snowflake

import (
	"crypto/rsa"
	"crypto/x509"
	"database/sql"
	"encoding/base64"
	"log"
	"os"

	"github.com/rwberendsen/grupr/internal/config"

	"github.com/snowflakedb/gosnowflake"
)

var db *sql.DB

func getDB() *sql.DB {
	if db != nil {
		return db
	}
	user := config.GetEnvOrDie("GRUPR_SNOWFLAKE_USER")
	role := config.GetEnvOrDie("GRUPR_SNOWFLAKE_ROLE")
	account := config.GetEnvOrDie("GRUPR_SNOWFLAKE_ACCOUNT")
	dbName := config.GetEnvOrDie("GRUPR_SNOWFLAKE_DB")
	useSQLOpen := config.GetEnvOrDie("GRUPR_SNOWFLAKE_USE_SQL_OPEN")

	// Not able to connect, whereas I was a while back;
	// Since then, company is now managing this device with JamF
	// Maybe issue with ZScaler?
	// Strategies to tackle:
	// - Check if TLS works on its own with Snowflake (elimination; hand-craft lower level code to get more info on what is going on)
	// - Try and get even more debugging info out of the Snowflake driver (already used Config with tracing and implemented RoundTrip interface logging the HTTP requests)
	// - Add ZScaler root key to Systems keychain (done with command):
	//     sudo security add-trusted-cert -d -r trustRoot -k /Library/Keychains/System.keychain ~/Downloads/Zscaler\ Root\ CA.pem
	// - Prove that it is ZScaler
	// - Checking OS stuff like sockets
	// - Play with Wireshark to see what is happening
	// - Check what is the TLS protocol version
	// Grokking the code and docs learning how it works
	// Use a different code platform (Python) to connect; this works

	var rsaKey *rsa.PrivateKey
	if useSQLOpen == "true" {
		dsn := user + "@" + account + "/" + dbName + "?authenticator=" + gosnowflake.AuthTypeExternalBrowser.String()
		log.Printf("dsn: %v", dsn)
		var err error
		db, err = sql.Open("snowflake", dsn)
		if err != nil {
			log.Fatalf("sql.Open error: %v", err)
		}
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
				log.Fatalf("getting rsa key: %v", err)
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
	rows, err := db.Query("SELECT CURRENT_USER()")
	if err != nil {
		if rsaKey != nil {
			log.Printf("please make sure public key is registered in Snowflake:")
			pubKeyByte, _ := x509.MarshalPKIXPublicKey(rsaKey.Public())
			log.Print(base64.StdEncoding.EncodeToString(pubKeyByte))
		}
		log.Fatalf("Error querying: %v", err)
	}
	for rows.Next() {
		var s string
		if err = rows.Scan(&s); err != nil {
			log.Fatalf("error scanning rows: %v", err)
		}
		log.Printf("connection is open with user %s", s)
	}
	if err = rows.Err(); err != nil {
		log.Fatalf("errors found during scanning: %s", err)
	}
	return db
}
