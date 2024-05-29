package semantics

import (
	"fmt"
)

type Interface struct {
	Matcher matcher
}

func (i Interface) validate() error {
	if m, err := newMatcher(i.Objects, i.ObjectsExclude); err != nil {
		return fmt.Errorf("invalid object matching expressions: %s", err)
	} else {
		i.Matcher = m
	}
	return nil
}

func (i Interface) equals(j Interface) bool {
	return i.Matcher.equals(j.Matcher)
}
