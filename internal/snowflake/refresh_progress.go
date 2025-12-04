package snowflake

type refreshProgressAccount struct {
	updated bool
	dbs map[string]*refreshProgressDB	
	importedDBs map[string]*refreshProgressDB
}

type refreshProgressDB struct {
	updated bool
	schemas map[string]*refreshProgressSchema
}

type refreshProgressSchema struct {
	updated bool
}
