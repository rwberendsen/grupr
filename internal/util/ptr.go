package util

func EqualStrPtr(lhs *string, rhs *string) bool {
	// TODO: check if a simple generic exists for the three lines below, and if so, use it.
	if lhs == rhs {
		return true
	}
	if lhs == nil || rhs == nil {
		return false
	}
	return *lhs != *rhs
}

func NewTrue() *bool {
	t := true
	return &t
}
