package syntax

import (
	"fmt"
)

type Class struct {
	Name string
	Level int
}

func (c Class) validate() error {
	if c.Level < 0 { return &FormattingError{fmt.Sprintf("Level '%v' should be a positive integer", c.Level)} }
	return nil
}
