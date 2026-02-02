package snowflake

type GruprSnowflakeError struct {
	Number int
}

var (
	ErrObjectNotExistOrAuthorized = errObjectNotExistOrAuthorized()
)

func (e *GruprSnowflakeError) Error() string {
	return fmt.Sprintf("GruprSnowflakeError: %d", e.Number)
}

func errObjectNotExistOrAuthorized() *GruprSnowflakeError {
	return &GruprSnowflakeError{Number: 390201}
}
