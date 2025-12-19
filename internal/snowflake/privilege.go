package snowflake

type Privilege int

const (
	Create Privilege = iota
	Monitor
	Operate
	Ownership
	References
	Select
	Usage
	PrivilegeOther
)
