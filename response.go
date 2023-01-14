package eventbus

import (
	"fmt"
	"strconv"

	"github.com/nats-io/nats.go"
)

type ResponseOption interface {
	configureResponse(e *Response)
}

type Response struct {
	Reply      string
	StatusCode int
	Data       []byte
	ErrorMsg   string
	Metadata   map[string]string
}

func NewResponse(reply string, data []byte, opts ...ResponseOption) Response {
	e := Response{
		Reply:      reply,
		StatusCode: StatusOK,
		Data:       data,
		Metadata:   make(map[string]string),
	}
	for _, option := range opts {
		option.configureResponse(&e)
	}
	return e
}

func (r Response) Err() error {
	if r.ErrorMsg != "" {
		return fmt.Errorf("statusCode: %d, errorMsg: %s", r.StatusCode, r.ErrorMsg)
	}
	return nil
}

func ResponseToNatsMsg(r Response) *nats.Msg {
	msg := nats.NewMsg(r.Reply)
	msg.Data = r.Data

	for k, v := range r.Metadata {
		msg.Header.Add(k, v)
	}
	msg.Header.Add(StatusCodeHeader, strconv.Itoa(r.StatusCode))
	msg.Header.Add(ErrorMessageHeader, r.ErrorMsg)
	return msg
}

func NatsMsgToResponse(msg *nats.Msg) Response {
	r := Response{
		Data:     msg.Data,
		Metadata: make(map[string]string),
	}

	for k := range msg.Header {
		switch k {
		case StatusCodeHeader:
			r.StatusCode, _ = strconv.Atoi(msg.Header.Get(k))
		case ErrorMessageHeader:
			r.ErrorMsg = msg.Header.Get(k)
		default:
			r.Metadata[k] = msg.Header.Get(k)
		}
	}

	return r
}

// * Response options **************************************************
type responseError struct {
	StatusCode int
	ErrorMsg   string
}

func (o responseError) configureResponse(r *Response) {
	r.StatusCode = o.StatusCode
	r.ErrorMsg = o.ErrorMsg
}

// WithResponseError adds status code and error message to response metadata
func WithResponseError(statusCode int, errorMsg string) responseError {
	return responseError{
		StatusCode: statusCode,
		ErrorMsg:   errorMsg,
	}
}

type responseInitiator struct {
	Initiator string
}

func (o *responseInitiator) configureResponse(r *Response) {
	r.Metadata[InitiatorMD] = o.Initiator
}

// WithResponseInitiator adds the Response initiator to the Response metadata
func WithResponseInitiator(initiator string) responseInitiator {
	return responseInitiator{
		Initiator: initiator,
	}
}
