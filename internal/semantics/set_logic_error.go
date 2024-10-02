package semantics

type SetLogicError struct {
	s string
}

func (e *SetLogicError) Error() string {
	return e.s
}
