package eventbus

const (
	// Event metadata common keys
	InitiatorMD string = "initiator"
	CreatedAtMD string = "createdAt"

	// NATS common headers
	StatusCodeHeader   string = "statusCode"
	ErrorMessageHeader string = "errorMessage"
	InitiatorHeader    string = "initiator"
	CreatedAtHeader    string = "createdAt"
	TraceIdHeader      string = "traceId"

	// NATS message response status codes
	StatusOK             int = 200
	StatusInvalidRequest int = 400
	StatusNotFound       int = 404
	StatusServerError    int = 500
)
