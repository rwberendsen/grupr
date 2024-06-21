package snowflake

import (
	"bytes"
	"crypto/rsa"
	"crypto/x509"
	"encoding/base64"
	"encoding/pem"
	"fmt"
	"os"
)

// Adapted from https://github.com/snowflakedb/gosnowflake/blob/5d28db80c2ffac67e9e8991eb2d78c591faedb47/priv_key_test.go#L23
// helper function to generate PKCS8 encoded base64 string of a private key
func generatePKCS8StringSupress(key *rsa.PrivateKey) string {
	// Error would only be thrown when the private key type is not supported
	// We would be safe as long as we are using rsa.PrivateKey
	tmpBytes, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		panic("marshalling valid RSA key failed")
	}
	privKeyPKCS8 := base64.URLEncoding.EncodeToString(tmpBytes)
	return privKeyPKCS8
}

func getPrivateRSAKey(keyPath string) (*rsa.PrivateKey, error) {
	data, err := os.ReadFile(keyPath)
	if err != nil {
		return nil, err
	}
	block, _ := pem.Decode(data) // ignores any remaining PEM formatted blocks
	if block == nil || block.Type != "PRIVATE KEY" {
		return nil, fmt.Errorf("%v is not a private key in PEM format.", keyPath)
	}
	parsedKey, _ := x509.ParsePKCS8PrivateKey(block.Bytes)
	privKey := parsedKey.(*rsa.PrivateKey) // will generate a run-time error if key is not an RSA key
	return privKey, nil
}

// Adapted from https://github.com/snowflakedb/gosnowflake/blob/5d28db80c2ffac67e9e8991eb2d78c591faedb47/priv_key_test.go#L64
// Helper function to add encoded private key to dsn
func appendPrivateKeyString(dsn string, key *rsa.PrivateKey) string {
	var b bytes.Buffer
	b.WriteString(dsn)
	b.WriteString(fmt.Sprintf("&authenticator=%v", "SNOWFLAKE_JWT"))
	b.WriteString(fmt.Sprintf("&privateKey=%s", generatePKCS8StringSupress(key)))
	return b.String()
}
