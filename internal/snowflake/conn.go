package snowflake

import (
	"crypto/x509"
	"database/sql"
	"encoding/base64"
	"log"

	"github.com/rwberendsen/grupr/internal/runtime"

	_ "github.com/snowflakedb/gosnowflake"
)

var db *sql.DB

func init() {
	user := runtime.GetEnvOrDie("SNOWFLAKE_USER")
	account := runtime.GetEnvOrDie("SNOWFLAKE_ACCOUNT")
	keyPath := runtime.GetEnvOrDie("SNOWFLAKE_ACCOUNT_RSA_KEY")
	dbName := runtime.GetEnvOrDie("SNOWFLAKE_DB")

	rsaKey, err := getPrivateRSAKey(keyPath)
	if err != nil {
		log.Fatalf("getting rsa key: %v", err)
	}
	dsn := user + "@" + account + "/" + dbName
	dsn = appendPrivateKeyString(dsn, rsaKey)
	db, err = sql.Open("snowflake", dsn)
	if err != nil {
		log.Fatalf("open db: %s", err)
	}
	rows, err := db.Query("SELECT CURRENT_USER()")
	if err != nil {
		log.Printf("please make sure public key is registered in Snowflake:")
		pubKeyByte, _ := x509.MarshalPKIXPublicKey(rsaKey.Public())
		log.Printf(base64.StdEncoding.EncodeToString(pubKeyByte))
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
}
