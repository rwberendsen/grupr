package snowflake

import (
	"crypto/rsa"
	"crypto/x509"
	"encoding/pem"
	"fmt"
	"os"
)

func getPrivateRSAKey(keyPath string) (*rsa.PrivateKey, error) {
	data, err := os.ReadFile(keyPath)
	if err != nil {
		return nil, err
	}
	block, _ := pem.Decode(data) // ignores any remaining PEM formatted blocks
	if block == nil || block.Type != "PRIVATE KEY" {
		return nil, fmt.Errorf("%v is not a private key in PEM format.", keyPath)
	}
	parsedKey, err := x509.ParsePKCS8PrivateKey(block.Bytes)
	if err != nil {
		return nil, err
	}
	privKey := parsedKey.(*rsa.PrivateKey) // will generate a run-time error if key is not an RSA key
	return privKey, nil
}
