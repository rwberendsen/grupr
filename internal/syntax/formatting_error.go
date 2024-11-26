package syntax

type FormattingError struct {
	s string
}

func (e *FormattingError) Error() string {
	return e.s
}
