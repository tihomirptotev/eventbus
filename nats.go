package eventbus

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/nats-io/nats.go"
	"golang.org/x/sync/errgroup"
)

type Logger interface {
	Infof(template string, args ...interface{})
	Warnf(template string, args ...interface{})
	Errorf(template string, args ...interface{})
}

type NatsBrokerConfig struct {
	NatsURL           string
	ConnectTimeout    time.Duration
	ReconnectWait     time.Duration
	TotalWait         time.Duration
	ReconnectBufSize  int
	PublishRetryWait  time.Duration
	RetryAttempts     int
	MaxWaitResp       time.Duration
	JsMessagesOutSize int
	LogStats          bool
	StatsInterval     time.Duration
}

func DefaultNatsBrokerConfig() *NatsBrokerConfig {
	return &NatsBrokerConfig{
		NatsURL:           "nats://localhost:4222",
		ConnectTimeout:    time.Second * 10,
		ReconnectWait:     time.Second,
		TotalWait:         time.Second * 300,
		ReconnectBufSize:  100 * 1024 * 1024,
		PublishRetryWait:  time.Millisecond * 50,
		RetryAttempts:     5,
		MaxWaitResp:       time.Second,
		JsMessagesOutSize: 2048,
		LogStats:          false,
		StatsInterval:     time.Second * 15,
	}
}

type NatsBroker struct {
	cfg           *NatsBrokerConfig
	logger        Logger
	nc            *nats.Conn
	js            nats.JetStreamContext
	errSubjectMap map[string]chan error
	mu            sync.Mutex
	tasks         map[string]func(ctx context.Context) error
	jsOut         chan *nats.Msg
	jsOutResults  chan nats.PubAckFuture
}

func NewNatsBroker(cfg *NatsBrokerConfig, logger Logger) (*NatsBroker, error) {

	broker := NatsBroker{
		cfg:           cfg,
		logger:        logger,
		errSubjectMap: make(map[string]chan error),
		tasks:         make(map[string]func(ctx context.Context) error),
		jsOut:         make(chan *nats.Msg, cfg.JsMessagesOutSize),
		jsOutResults:  make(chan nats.PubAckFuture, cfg.JsMessagesOutSize),
	}

	natsErrHandler := func(nc *nats.Conn, sub *nats.Subscription, natsErr error) {
		defer func() {
			if r := recover(); r != nil {
				logger.Errorf("nats: recovered in natsErrHandler: %v", r)
			}
		}()

		logger.Errorf("nats: subject = %s: %v", sub.Subject, natsErr)

		broker.mu.Lock()
		errCh, ok := broker.errSubjectMap[sub.Subject]
		broker.mu.Unlock()

		if ok {
			errCh <- natsErr
		}

	}

	nc, err := nats.Connect(
		cfg.NatsURL,
		nats.Timeout(cfg.ConnectTimeout),
		nats.ReconnectWait(cfg.ReconnectWait),
		nats.MaxReconnects(int(cfg.TotalWait/cfg.ReconnectWait)),
		nats.ReconnectBufSize(cfg.ReconnectBufSize),
		nats.ErrorHandler(natsErrHandler),
		nats.DisconnectErrHandler(func(c *nats.Conn, err error) {
			logger.Errorf("nats: disconected: %v", err)
		}),
		nats.ReconnectHandler(func(c *nats.Conn) {
			logger.Infof("nats: reconnected: %s ....", c.ConnectedUrl())
		}),
	)
	if err != nil {
		return nil, fmt.Errorf("nats: create client: %w", err)
	}
	broker.nc = nc

	broker.js, err = broker.nc.JetStream(nats.MaxWait(cfg.MaxWaitResp), nats.PublishAsyncMaxPending(cfg.JsMessagesOutSize/2))
	if err != nil {
		return nil, fmt.Errorf("nats: create js context: %w", err)
	}

	return &broker, nil
}

func (b *NatsBroker) Request(msg *nats.Msg, timeout time.Duration) (*nats.Msg, error) {
	return b.nc.RequestMsg(msg, timeout)
}

func (b *NatsBroker) Subscribe(subject string, f nats.MsgHandler) {
	b.nc.Subscribe(subject, f)
}

func (b *NatsBroker) QueueSubscribe(subject, queue string, f nats.MsgHandler) {
	b.nc.QueueSubscribe(subject, queue, f)
}

func (b *NatsBroker) CreateJetStream(cfg *nats.StreamConfig, opts ...nats.JSOpt) (*nats.StreamInfo, error) {
	return b.js.AddStream(cfg, opts...)
}

func (b *NatsBroker) CreateKVStore(bucket string) (nats.KeyValue, error) {
	kv, err := b.js.CreateKeyValue(&nats.KeyValueConfig{
		Bucket:  bucket,
		Storage: nats.FileStorage,
	})
	if err != nil {
		return nil, fmt.Errorf("nats: create kv store: %w", err)
	}
	return kv, nil
}

func (b *NatsBroker) GetKVStore(bucket string) (nats.KeyValue, error) {
	kv, err := b.js.KeyValue(bucket)
	if err != nil {
		return nil, fmt.Errorf("nats: get kv store: %w", err)
	}
	return kv, nil
}

func (b *NatsBroker) JsPublishMsg(msg *nats.Msg, opts ...nats.PubOpt) error {
	_, err := b.js.PublishMsg(msg, opts...)
	return err
}

func (b *NatsBroker) JsPublishMsgAsync(msg *nats.Msg) {
	b.jsOut <- msg
}

func (b *NatsBroker) Close() {
	b.nc.LastError()
	b.nc.Drain()
	close(b.jsOut)
	close(b.jsOutResults)
}

func (b *NatsBroker) Run(ctx context.Context) error {
	g := errgroup.Group{}

	// * Run subscription tasks
	for k, v := range b.tasks {
		task := v
		subject := k
		g.Go(func() error {
			for {
				err := task(ctx)
				if err != nil {
					b.logger.Errorf("nats: process subject %s: %v: trying to reconnect...", subject, err)
					continue
				}
				return nil
			}
		})
	}

	var jsPubErrors uint64

	// * Handle jetstream async publish results
	g.Go(func() error {
		defer func() {
			if r := recover(); r != nil {
				b.logger.Errorf("nats: recovered in handle jetstream async publish results: %v", r)
			}
		}()

		handleAck := func(ack nats.PubAckFuture) {
			select {
			case <-ack.Ok():
				return
			case err := <-ack.Err():
				atomic.AddUint64(&jsPubErrors, 1)
				b.logger.Errorf("nats: jetstream publish ack: %v", err)
				b.jsOut <- ack.Msg()
			}
		}

		for {
			select {
			case <-ctx.Done():
				t := time.NewTimer(time.Second * 3)
				defer t.Stop()
				for {
					select {
					case ack, ok := <-b.jsOutResults:
						if !ok {
							return nil
						}
						handleAck(ack)
					case <-t.C:
						b.logger.Warnf("nats: timeout draining js async publish results channel")
						return nil
					}
				}
			case ack, ok := <-b.jsOutResults:
				if !ok {
					return nil
				}
				handleAck(ack)
			}
		}
	})

	// * Publish async jetstream messages
	g.Go(func() error {
		defer func() {
			if r := recover(); r != nil {
				b.logger.Errorf("nats: recovered in publish async jetstream messages: %v", r)
			}
		}()

		publish := func(msg *nats.Msg) {
			ack, err := b.js.PublishMsgAsync(msg)
			if err != nil {
				atomic.AddUint64(&jsPubErrors, 1)
				b.logger.Errorf("nats: jetstream async publish msg: %v", err)
				b.jsOut <- msg
				return
			}
			b.jsOutResults <- ack
		}

		for {
			select {
			case <-ctx.Done():
				t := time.NewTimer(time.Second * 3)
				defer t.Stop()
				for {
					select {
					case msg, ok := <-b.jsOut:
						if !ok {
							return nil
						}
						publish(msg)
					case <-t.C:
						b.logger.Warnf("nats: timeout draining js async publish channel")
						return nil
					}
				}
			case msg, ok := <-b.jsOut:
				if !ok {
					return nil
				}
				publish(msg)

			}
		}
	})

	// * Run broker stats
	if b.cfg.LogStats {
		g.Go(func() error {
			t := time.NewTicker(time.Second * 15)
			defer t.Stop()

			for {
				select {
				case <-ctx.Done():
					return nil
				case <-t.C:
					jsPubPending := len(b.jsOut)
					jsPubResults := len(b.jsOutResults)
					jsAckPending := b.js.PublishAsyncPending()
					b.logger.Infof(
						"nats jetstream stats: msgQueueSize = %d, ackPending = %d, resQueueSize = %d, jsPubErrors = %d",
						jsPubPending,
						jsAckPending,
						jsPubResults,
						atomic.LoadUint64(&jsPubErrors))
				}
			}
		})
	}

	return g.Wait()
}

func NatsMsgToString(m *nats.Msg) string {
	return fmt.Sprintf("nats message: subject = %s: payload = %s: headers = %#v", m.Subject, string(m.Data), m.Header)
}
