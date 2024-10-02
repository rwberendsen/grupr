package semantics

type PolicyError struct {
	s string
}

func (e *PolicyError) Error() string {
	return e.s
}
