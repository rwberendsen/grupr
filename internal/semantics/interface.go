package semantics

import (
	"fmt"

	"github.com/rwberendsen/grupr/internal/syntax"
)

type Interface struct {
	ObjectMatcher ObjectMatcher
	InterfaceMetadata
}

func newInterface(i syntax.Interface, DTAPs map[string]KindOfData, UserGroups map[string]bool) (Interface, error) {
	if m, err := newObjectMatcher(i.Objects, i.ObjectsExclude, DTAPs, UserGroups); err != nil {
		return Interface{}, fmt.Errorf("invalid object matching expressions: %s", err)
	} else {
		return Interface{m}, nil
	}
}

func (i Interface) equals(j Interface) bool {
	return i.ObjectMatcher.equals(j.ObjectMatcher)
}
