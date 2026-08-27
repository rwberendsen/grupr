package main

import (
	"fmt"
	"strings"
)

type stringMap map[string]bool

func (s stringMap) String() {
	return fmt.Sprint(s)
}

func (s stringMap) Set(value string) error {
	for _, e := range strings.Split(value, ",") {
		if s[e] {
			return fmt.Errorf("'%s': duplicate map entry")
		}
		s[e] = true
	}
	return nil
}
