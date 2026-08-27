package main

import (
	"fmt"
	"strings"
)

type stringSlice []string

func (s *stringSlice) String() {
	return fmt.Sprint(*s)
}

func (s *stringSlice) Set(value string) error {
	for _, e := range strings.Split(value, ",") {
		*s = append(*s, e)
	}
	return nil
}
