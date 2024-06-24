package snowflake

import (
	"crypto/rsa"
	"crypto/x509"
	"database/sql"
	"encoding/base64"
	"log"
	"net/http"
	"os"

	"github.com/rwberendsen/grupr/internal/runtime"

	"github.com/snowflakedb/gosnowflake"
)

var db *sql.DB

type loggingTransport struct{}

func (t *loggingTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	log.Printf("----REQUEST START:")
	log.Printf("%v", r)
	log.Printf("----REQUEST END:")
	res, err := gosnowflake.SnowflakeTransport.RoundTrip(r)
	log.Printf("----RESPONSE START:")
	log.Printf("%v", res)
	log.Printf("----RESPONSE END:")
	return res, err
}

func init() {
	user := runtime.GetEnvOrDie("SNOWFLAKE_USER")
	account := runtime.GetEnvOrDie("SNOWFLAKE_ACCOUNT")
	dbName := runtime.GetEnvOrDie("SNOWFLAKE_DB")
	region := runtime.GetEnvOrDie("SNOWFLAKE_REGION")

	var cnf *gosnowflake.Config
	var rsaKey *rsa.PrivateKey
	if keyPath, ok := os.LookupEnv("SNOWFLAKE_ACCOUNT_RSA_KEY"); !ok {
		cnf = &gosnowflake.Config{
			Account:       account,
			User:          user,
			Database:      dbName,
			Region:        region,
			Authenticator: gosnowflake.AuthTypeExternalBrowser,
			Tracing:       "trace",
			Transporter:   &loggingTransport{},
		}
	} else {
		rsaKey, err := getPrivateRSAKey(keyPath)
		if err != nil {
			log.Fatalf("getting rsa key: %v", err)
		}
		cnf = &gosnowflake.Config{
			Account:       account,
			User:          user,
			Database:      dbName,
			Region:        region,
			Authenticator: gosnowflake.AuthTypeJwt,
			PrivateKey:    rsaKey,
			Tracing:       "trace",
			Transporter:   &loggingTransport{},
		}
	}
	connector := gosnowflake.NewConnector(gosnowflake.SnowflakeDriver{}, *cnf)
	db = sql.OpenDB(connector)
	rows, err := db.Query("SELECT CURRENT_USER()")
	if err != nil {
		if rsaKey != nil {
			log.Printf("please make sure public key is registered in Snowflake:")
			pubKeyByte, _ := x509.MarshalPKIXPublicKey(rsaKey.Public())
			log.Printf(base64.StdEncoding.EncodeToString(pubKeyByte))
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
}
