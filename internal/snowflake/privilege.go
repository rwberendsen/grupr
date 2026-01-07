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

func joinPrivileges(

func (p Privilege) String() string {
	m := map[Privilege]string{
		Create: "CREATE",
		Monitor: "MONITOR",
		Operate: "OPERATE",
		Ownership: "OWNERSHIP",
		References: "REFERENCES",
		Select: "SELECT",
		Usage: "USAGE",
		PrivilegeOther: "OTHER",
	}
	return m[p]
}
