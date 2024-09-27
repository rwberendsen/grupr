package syntax

import (
	"fmt"
)

type UserGroups struct {
	Current []string
	Divested []string
}

func (u UserGroups) validate() error {
	allGroups := map[string]bool{}
	for _, i := range Current {
		if err := validateID(i); err != nil { return fmt.Errorf("current user groups: %v", err) }
		if _, ok := allGroups[i]; ok { return fmt.Errorf("duplicate user group: %s", i) }
	}
	for _, i := range Divested {
		if err := validateID(i); err != nil { return fmt.Errorf("divested user groups: %v", err) }
		if _, ok := allGroups[i]; ok { return fmt.Errorf("duplicate user group: %s", i) }
	}
}
