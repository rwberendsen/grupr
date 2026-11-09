package snowflake

import (
	"errors"
	"fmt"
	"strings"

	"github.com/snowflakedb/gosnowflake"
)

// We sometimes raise a "Snowflake-like" error ourselves,
// for example when during querying objects we noticed some
// objects dissappeared, and we report an ErrObjectNotExistOrAuthorized error
type GruprSnowflakeError struct {
	Number int
}

const (
	// Observed Snowflake error numbers for which they don't have error constants
	ErrRoleNotExistOrAuthorized      = 2003
	ErrParquetUnloadDataTypeMismatch = 100171
)

func (e *GruprSnowflakeError) Error() string {
	return fmt.Sprintf("GruprSnowflakeError: %d", e.Number)
}

func newGruprSnowflakeError(no int) *GruprSnowflakeError {
	return &GruprSnowflakeError{Number: no}
}

func FmtErr(err error) string {
	// Check if there is a Snowflake(-like) error, and print it before printing the final error message
	var b strings.Builder
	if sfErr, ok := errors.AsType[*gosnowflake.SnowflakeError](err); ok {
		fmt.Fprintf(&b, "...\n")
		fmt.Fprintf(&b, "SnowflakeError:\n")
		fmt.Fprintf(&b, "Number:   %d\n", sfErr.Number)
		fmt.Fprintf(&b, "SQLState: %s\n", sfErr.SQLState)
		fmt.Fprintf(&b, "QueryID:  %s\n", sfErr.QueryID)
		fmt.Fprintf(&b, "Message:  %s ...\n", sfErr.Message)
	}
	if gsfErr, ok := errors.AsType[*GruprSnowflakeError](err); ok {
		fmt.Fprintf(&b, "...\n")
		fmt.Fprintf(&b, "%v ...\n", gsfErr)
	}
	fmt.Fprintf(&b, "%v\n", err)
	return b.String()
}

func IsErr(err error, no int) bool {
	// Check if this is a Snowflake(-like) error and if so, if the error numbers match
	if gsfErr, ok := errors.AsType[*GruprSnowflakeError](err); ok {
		return gsfErr.Number == no
	}
	if sfErr, ok := errors.AsType[*gosnowflake.SnowflakeError](err); ok {
		return sfErr.Number == no
	}
	return false
}
