package syntax

type FormattingError struct {
	s string
}

func (e *FormattingError) Error() struct {
	return e.s
}
