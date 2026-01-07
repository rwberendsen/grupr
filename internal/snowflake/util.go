package snowflake

import (
	"fmt"
)

func printSQL(sql string, params ...any) {
	fmt.Print(sql, "; ")
	for param := range params {
		fmt.Print(", ", param)
	}
	fmt.Print("\n")
}

