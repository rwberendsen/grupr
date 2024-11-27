package syntax

type FormattingError struct {
	S string
}

func (e *FormattingError) Error() string {
	return e.S
}
