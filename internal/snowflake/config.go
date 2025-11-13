package snowflake

import os

type Config struct {
	User string
	Role string
	Account string
	Database string
	Schema string
	UseSQLOpen bool
	ObjectPrefix string // for objects created by Grupr in Snowflake
}

func GetConfig() *Config, error {
	cnf := new(Config)

	user, ok := os.LookupEnv("GRUPR_SNOWFLAKE_USER")
	if !ok { return nil, fmt.Errorf("Could not find environment variable GRUPR_SNOWFLAKE_USER") }
	cnf.User = user

	role, ok := os.LookupEnv("GRUPR_SNOWFLAKE_ROLE")
	if !ok { return nil, fmt.Errorf("Could not find environment variable GRUPR_SNOWFLAKE_USER") }
	cnf.Role = role

	account, ok := os.LookupEnv("GRUPR_SNOWFLAKE_ACCOUNT")
	if !ok { return nil, fmt.Errorf("Could not find environment variable GRUPR_SNOWFLAKE_ACCOUNT") }
	cnf.Account = account

	database, ok := os.LookupEnv("GRUPR_SNOWFLAKE_DB")
	if !ok { return nil, fmt.Errorf("Could not find environment variable GRUPR_SNOWFLAKE_DB") }
	cnf.Database = database

	useSQLOpen, ok := os.LookupEnv("GRUPR_SNOWFLAKE_USE_SQL_OPEN")
	if !ok { return nil, fmt.Errorf("Could not find environment variable GRUPR_SNOWFLAKE_USE_SQL_OPEN") }
	if useSQLOpen == "true" {
		cnf.UseSQLOpen = true
	} else if useSQLOpen == "false" {
		cnf.UseSQLOpen = false
	}	
}
