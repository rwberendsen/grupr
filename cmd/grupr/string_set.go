package main

import (
	"fmt"
	"strings"
)

type stringSet map[string]bool

func (s stringSet) String() string {
	return fmt.Sprint(map[string]bool(s))
}

func (s stringSet) Set(value string) error {
	for _, e := range strings.Split(value, ",") {
		if s[e] {
			return fmt.Errorf("'%s': duplicate map entry")
		}
		s[e] = true
	}
	return nil
}
