package eventbus

import (
	"encoding/json"
	"time"

	"github.com/nats-io/nats.go"
)

type EventOption interface {
	ConfigureEvent(e *Event)
}

type Event struct {
	Name      string
	Payload   any
	Metadata  map[string]string
	CreatedAt time.Time
}

func NewEvent(name string, payload any, opts ...EventOption) Event {
	e := Event{
		Name:      name,
		Payload:   payload,
		Metadata:  make(map[string]string),
		CreatedAt: time.Now().UTC(),
	}
	for _, option := range opts {
		option.ConfigureEvent(&e)
	}
	return e
}

type eventInitiator struct {
	Initiator string
}

func (o *eventInitiator) ConfigureEvent(e *Event) {
	e.Metadata[InitiatorMD] = o.Initiator
}

// WithEventinitiator adds the event initiator the event metadata
func WithEventinitiator(initiator string) eventInitiator {
	return eventInitiator{
		Initiator: initiator,
	}
}

func EventToNatsMsg(e Event) (*nats.Msg, error) {
	msg := nats.NewMsg(e.Name)
	for k, v := range e.Metadata {
		msg.Header.Add(k, v)
	}
	msg.Header.Add(CreatedAtHeader, e.CreatedAt.Format(time.RFC3339Nano))
	data, err := json.Marshal(e.Payload)
	if err != nil {
		return nil, err
	}
	msg.Data = data
	return msg, nil
}
