package natstest

import (
	"bytes"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/tihomirptotev/eventbus"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

func createLogger(t *testing.T) *zap.SugaredLogger {
	t.Helper()
	l, err := zap.NewDevelopment()
	require.NoError(t, err)
	return l.Sugar()
}

func createBroker(t *testing.T) *eventbus.NatsBroker {
	logger := createLogger(t)
	cfg := eventbus.DefaultNatsBrokerConfig()
	broker, err := eventbus.NewNatsBroker(cfg, logger)
	require.NoError(t, err)
	return broker

}

func TestNatsRequestResponse(t *testing.T) {
	broker := createBroker(t)
	t.Cleanup(func() {
		broker.Close()
	})

	t.Run("simple request", func(t *testing.T) {
		var received int64
		subject := "test-req"
		broker.Subscribe(subject, func(msg *nats.Msg) {
			t.Logf("request received: %v", eventbus.NatsMsgToString(msg))
			atomic.AddInt64(&received, 1)
			buf := new(bytes.Buffer)
			buf.Write(msg.Data)
			buf.WriteString(" - resp")
			resp := eventbus.NewResponse(msg.Reply, buf.Bytes())
			msg.RespondMsg(eventbus.ResponseToNatsMsg(resp))
		})

		data := []byte(fmt.Sprintf("request: %d", 1))
		req := eventbus.NewRequest(subject, data)
		msg := eventbus.RequestToNatsMsg(req)
		resp, err := broker.Request(msg, time.Second)
		assert.NoError(t, err)
		assert.Contains(t, string(resp.Data), " - resp")
		t.Logf("response received: %v", eventbus.NatsMsgToString(resp))

		assert.Equal(t, 1, int(received))

	})

	t.Run("multiple requests processed one by one", func(t *testing.T) {
		var received int64
		subject := "test-req-multiple-one-by-one"
		broker.Subscribe(subject, func(msg *nats.Msg) {
			atomic.AddInt64(&received, 1)
			buf := new(bytes.Buffer)
			buf.Write(msg.Data)
			buf.WriteString(" - resp")
			resp := eventbus.NewResponse(msg.Reply, buf.Bytes())

			// simulate slow function
			time.Sleep(time.Millisecond * 100)

			msg.RespondMsg(eventbus.ResponseToNatsMsg(resp))
		})

		start := time.Now()
		count := 5
		for i := 0; i < count; i++ {
			data := []byte(fmt.Sprintf("request: %d", i))
			req := eventbus.NewRequest(subject, data)
			msg := eventbus.RequestToNatsMsg(req)
			resp, err := broker.Request(msg, time.Second)
			assert.NoError(t, err)
			assert.Contains(t, string(resp.Data), " - resp")
		}

		t.Logf("total time one by one: %v", time.Since(start))

		assert.Equal(t, count, int(received))

	})

	t.Run("multiple requests processed concurrently", func(t *testing.T) {
		count := 5
		var received int64
		subject := "test-req-multiple-concurently"
		g := errgroup.Group{}
		broker.Subscribe(subject, func(msg *nats.Msg) {
			atomic.AddInt64(&received, 1)
			buf := new(bytes.Buffer)
			buf.Write(msg.Data)
			buf.WriteString(" - resp")
			resp := eventbus.NewResponse(msg.Reply, buf.Bytes())

			// simulate slow function
			time.Sleep(time.Millisecond * 100)
			msg.RespondMsg(eventbus.ResponseToNatsMsg(resp))
		})

		start := time.Now()
		for i := 0; i < count; i++ {
			i := i
			g.Go(func() error {
				data := []byte(fmt.Sprintf("request: %d", i))
				req := eventbus.NewRequest(subject, data)
				msg := eventbus.RequestToNatsMsg(req)
				resp, err := broker.Request(msg, time.Second)
				assert.NoError(t, err)
				assert.Contains(t, string(resp.Data), " - resp")
				return nil
			})
		}

		require.NoError(t, g.Wait())

		t.Logf("total time concurently: %v", time.Since(start))

		assert.Equal(t, count, int(received))

	})

	t.Run("request-response timeout", func(t *testing.T) {
		subject := "test-req-timeout"
		timeout := time.Millisecond * 100
		broker.Subscribe(subject, func(msg *nats.Msg) {
			time.Sleep(timeout * 2)
			resp := eventbus.NewResponse(msg.Reply, []byte("ok"))
			msg.RespondMsg(eventbus.ResponseToNatsMsg(resp))
		})

		data := []byte("request: 1")
		req := eventbus.NewRequest(subject, data)
		msg := eventbus.RequestToNatsMsg(req)
		_, err := broker.Request(msg, timeout)
		assert.ErrorIs(t, err, nats.ErrTimeout)
	})
}
