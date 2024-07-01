package config

import (
	"log"
	"os"
)

func GetEnvOrDie(key string) string {
	val, ok := os.LookupEnv(key)
	if !ok {
		log.Fatalf("env var not found: %s", key)
	}
	return val
}

func GetEnvOrDefault(key string, default_ string) string {
	val, ok := os.LookupEnv(key)
	if !ok {
		return default_
	}
	return val
}
