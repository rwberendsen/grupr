package util

import (
	"fmt"
)

func FmtSliceElements[T any](in ...T) []string {
	out := make([]string, len(in))
	for i, v := range in {
		out[i] = fmt.Sprintf("%v", v)
	}
	return out
}
