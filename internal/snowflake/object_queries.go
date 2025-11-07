package snowflake

func queryDBs() (map[string]bool, error) {
	dbs := map[string]bool{}
	start := time.Now()
	log.Printf("Querying Snowflake for database names...\n")
	rows, err := getDB().Query(`SHOW TERSE DATABASES IN ACCOUNT ->> SELECT "name" FROM S1`)
	if err != nil {
		return nil, fmt.Errorf("queryDBs error: %w", err)
	}
	for rows.Next() {
		var dbName string
		if err = rows.Scan(&dbName); err != nil {
			return nil, fmt.Errorf("queryDBs: error scanning row: %w", err)
		}
		if _, ok := dbs[dbName]; ok { return nil, fmt.Errorf("duplicate db name: %s", dbName) }
		dbs[dbName] = true
	}
	if err = rows.Err(); err != nil {
		return nil, fmt.Errorf("queryDBs: error after looping over results: %w", err)
	}
	t := time.Now()
	log.Printf("Querying Snowflake for database names took %v\n", t.Sub(start))
	return dbs, nil
}
