package eventbus

import (
	"time"

	"github.com/google/uuid"
	"github.com/nats-io/nats.go"
)

type RequestOption interface {
	configureRequest(e *Request)
}

type Request struct {
	ID        string
	Subject   string
	Data      []byte
	Metadata  map[string]string
	CreatedAt time.Time
}

func NewRequest(subject string, data []byte, opts ...RequestOption) Request {
	e := Request{
		ID:        uuid.NewString(),
		Subject:   subject,
		Data:      data,
		Metadata:  make(map[string]string),
		CreatedAt: time.Now().UTC(),
	}
	for _, option := range opts {
		option.configureRequest(&e)
	}
	return e
}

// * Request options **************************************************

type requestInitiator struct {
	Initiator string
}

func (o *requestInitiator) configureRequest(e *Request) {
	e.Metadata[InitiatorMD] = o.Initiator
}

// WithRequestInitiator adds the Request initiator to the Request metadata
func WithRequestInitiator(initiator string) requestInitiator {
	return requestInitiator{
		Initiator: initiator,
	}
}

func RequestToNatsMsg(r Request) *nats.Msg {
	msg := nats.NewMsg(r.Subject)
	msg.Data = r.Data
	for k, v := range r.Metadata {
		msg.Header.Add(k, v)
	}
	msg.Header.Add(CreatedAtHeader, r.CreatedAt.Format(time.RFC3339Nano))
	msg.Header.Add(TraceIdHeader, r.ID)
	return msg
}
