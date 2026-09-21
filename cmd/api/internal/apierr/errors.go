package apierr

import "fmt"

// APIError represents an operational HTTP error matching TS ApiError.
type APIError struct {
	StatusCode int    `json:"-"`
	Code       string `json:"code"`
	Message    string `json:"message"`
}

func (e *APIError) Error() string {
	return e.Message
}

func New(statusCode int, code, message string) *APIError {
	return &APIError{
		StatusCode: statusCode,
		Code:       code,
		Message:    message,
	}
}

func ResourceNotFound(resource, identifier string) *APIError {
	return New(404, "RESOURCE_NOT_FOUND", fmt.Sprintf("%s '%s' was not found", resource, identifier))
}

func InvalidCursor() *APIError {
	return New(400, "INVALID_CURSOR", "The pagination cursor is invalid")
}

func Conflict(message string) *APIError {
	return New(409, "CONFLICT", message)
}

func InvalidStateTransition(from, to string) *APIError {
	return New(422, "INVALID_STATE_TRANSITION", fmt.Sprintf("Cannot transition job from '%s' to '%s'", from, to))
}

func Forbidden(message string) *APIError {
	if message == "" {
		message = "Insufficient permissions"
	}
	return New(403, "FORBIDDEN", message)
}

func Unauthorized(message string) *APIError {
	if message == "" {
		message = "Missing or invalid authentication token"
	}
	return New(401, "UNAUTHORIZED", message)
}

func ValidationError(message string) *APIError {
	return New(400, "VALIDATION_ERROR", message)
}

