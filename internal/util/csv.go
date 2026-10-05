package util

// We use encoding/csv, and sometimes need the information downstream
// whether or not a field was quoted before parsing
type StringWasQuoted struct {
	S         string
	WasQuoted bool
}
