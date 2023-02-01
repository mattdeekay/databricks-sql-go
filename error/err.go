package error

import (
	"context"
	"fmt"
	"github.com/databricks/databricks-sql-go/driverctx"
	"github.com/pkg/errors"
)

// Error messages
const (
	// System Fault (driver errors, system failures)
	ErrNotImplemented           = "not implemented"
	ErrTransactionsNotSupported = "transactions are not supported"
	ErrParametersNotSupported   = "query parameters are not supported"
	ErrInvalidOperationState    = "invalid operation state. This should not have happened"

	ErrReadQueryStatus = "could not read query status"

	// Execution error messages (query failure)
	ErrQueryExecution = "failed to execute query"

	// Request error messages (connection, authentication, network error)
	ErrCloseConnection = "failed to close connection"
	ErrThriftClient    = "error initializing thrift client"
	ErrInvalidURL      = "invalid URL"

	ErrNoAuthenticationMethod = "no authentication method set"
	ErrInvalidDSNFormat       = "invalid DSN: invalid format"
	ErrInvalidDSNPort         = "invalid DSN: invalid DSN port"
	ErrInvalidDSNTokenIsEmpty = "invalid DSN: empty token"
	ErrBasicAuthNotSupported  = "invalid DSN: basic auth not enabled"
	ErrInvalidDSNMaxRows      = "invalid DSN: maxRows param is not an integer"
	ErrInvalidDSNTimeout      = "invalid DSN: timeout param is not an integer"
)

type DatabricksError interface {
	Error() string
	ErrorType() string
	CorrelationId() string
	ConnectionId() string
	Message() string
	ErrorCondition() string
}

type databricksError struct {
	msg     string
	err     error
	corrId  string
	connId  string
	errType dbsqlErrorType
}

func newDatabricksError(ctx context.Context, msg string, err error, errType dbsqlErrorType) databricksError {
	return databricksError{
		msg:     msg,
		err:     errors.WithStack(err),
		corrId:  driverctx.CorrelationIdFromContext(ctx),
		connId:  driverctx.ConnIdFromContext(ctx),
		errType: errType,
	}
}

type dbsqlErrorType int64

// Error types
const (
	Unknown dbsqlErrorType = iota
	Request
	Execution
	System
)

func (t dbsqlErrorType) string() string {
	switch t {
	case Request:
		return "request error"
	case Execution:
		return "execution error"
	case System:
		return "system fault"
	}
	return "unknown"
}

type DatabricksErrorWithQuery interface {
	QueryId() string
	ErrorCondition() string
}

func (e databricksError) Error() string {
	return fmt.Sprintf("databricks: %s: %s: %v", e.errType.string(), e.msg, e.err)
}

func (e databricksError) Unwrap() error {
	return e.err
}

func (e databricksError) Message() string {
	return e.msg
}

func (e databricksError) CorrelationId() string {
	return e.corrId
}

func (e databricksError) ConnectionId() string {
	return e.connId
}

func (e databricksError) ErrorType() string {
	return e.errType.string()
}

// SystemFault are issues with the driver or server, e.g. not supported operations, driver specific non-recoverable failures
type SystemFault struct {
	databricksError
	isRetryable bool
}

func (e *SystemFault) IsRetryable() bool {
	return e.isRetryable
}

func NewSystemFault(ctx context.Context, msg string, err error) *SystemFault {
	dbsqlErr := newDatabricksError(ctx, msg, err, System)
	return &SystemFault{dbsqlErr, false}
}

// RequestError are errors caused by invalid requests, e.g. permission denied, warehouse not found
type RequestError struct {
	databricksError
}

func NewRequestError(ctx context.Context, msg string, err error) *RequestError {
	return &RequestError{newDatabricksError(ctx, msg, err, Request)}
}

// ExecutionError are errors occurring after the query has been submitted, e.g. invalid syntax, query timeout
type ExecutionError struct {
	databricksError
	queryId  string
	errClass string
	sqlState string
}

func (q *ExecutionError) QueryId() string {
	return q.queryId
}

func (q *ExecutionError) ErrorClass() string {
	return q.errClass
}

func (q *ExecutionError) SqlState() string {
	return q.sqlState
}

func NewExecutionError(ctx context.Context, msg string, err error, sqlState string) *ExecutionError {
	dbsqlErr := newDatabricksError(ctx, msg, err, Execution)
	errClass := ""
	return &ExecutionError{dbsqlErr, driverctx.QueryIdFromContext(ctx), errClass, sqlState}
}
