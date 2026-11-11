// Package kafka — источник событий notifier из Kafka (franz-go) и публикация в DLQ.
//
// Consumer group notifier, at-least-once. Сообщения обрабатываются параллельно (письмо идёт ~1 с, последовательно
// поток бы не успевал), а смещение партиции фиксируется по «водяной отметке»: до первого ещё не обработанного
// сообщения. Медленное письмо или повторы одного сообщения не задерживают остальные, но и не дают закоммитить
// то, что не обработано: после падения придут повторы (их гасит идемпотентность), потерь нет.
// Порядок гарантирован только внутри ключа (order_id) — и только до параллельной обработки (ADR-003).
package kafka

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/nikolaysavelev/soldout/internal/notification/app"
)

// Options — настройки потребителя.
type Options struct {
	Brokers     []string
	Topic       string // outbox.event.order
	DLQTopic    string // outbox.event.order.dlq
	Group       string // notifier
	Concurrency int    // одновременно обрабатываемых сообщений (backpressure: опрос ждёт свободный слот)
}

// Consumer — источник событий.
type Consumer struct {
	cl       *kgo.Client
	adm      *kadm.Client
	opts     Options
	logger   *slog.Logger
	tracker  *tracker
	inflight sync.WaitGroup
}

// New подключается к Kafka.
func New(opts Options, logger *slog.Logger) (*Consumer, error) {
	if opts.Concurrency <= 0 {
		opts.Concurrency = 256
	}
	c := &Consumer{opts: opts, logger: logger, tracker: newTracker()}
	cl, err := kgo.NewClient(
		kgo.SeedBrokers(opts.Brokers...),
		kgo.ConsumerGroup(opts.Group),
		kgo.ConsumeTopics(opts.Topic),
		kgo.AutoCommitMarks(), // коммитятся только отмеченные смещения (водяная отметка), раз в секунду
		kgo.AutoCommitInterval(time.Second),
		kgo.FetchMaxWait(100*time.Millisecond),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		kgo.ClientID("notifier"),
		// перед отдачей партиций (рестарт, ребаланс) ждём начатые сообщения и коммитим отметку
		kgo.OnPartitionsRevoked(func(ctx context.Context, cl *kgo.Client, _ map[string][]int32) {
			c.inflight.Wait()
			if err := cl.CommitMarkedOffsets(ctx); err != nil {
				logger.Warn("kafka: commit при отзыве партиций", "err", err)
			}
		}),
	)
	if err != nil {
		return nil, fmt.Errorf("kafka: клиент: %w", err)
	}
	c.cl, c.adm = cl, kadm.NewClient(cl)
	return c, nil
}

// Close — фиксирует отмеченные смещения и закрывает клиента.
func (c *Consumer) Close() {
	c.inflight.Wait()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = c.cl.CommitMarkedOffsets(ctx)
	c.cl.Close()
}

// Ping — проверка соединения с брокером (readyz).
func (c *Consumer) Ping(ctx context.Context) error { return c.cl.Ping(ctx) }

// Run — цикл опроса до отмены ctx.
func (c *Consumer) Run(ctx context.Context, handle func(context.Context, app.Message) error) error {
	sem := make(chan struct{}, c.opts.Concurrency)
	for {
		fetches := c.cl.PollRecords(ctx, c.opts.Concurrency)
		if ctx.Err() != nil {
			c.inflight.Wait()
			return nil
		}
		fetches.EachError(func(t string, p int32, err error) {
			c.logger.WarnContext(ctx, "kafka: ошибка опроса", "topic", t, "partition", p, "err", err)
		})
		for _, r := range fetches.Records() {
			select {
			case sem <- struct{}{}:
			case <-ctx.Done():
				c.inflight.Wait()
				return nil
			}
			c.tracker.start(r)
			c.inflight.Add(1)
			go func(r *kgo.Record) {
				defer func() { <-sem; c.inflight.Done() }()
				if err := handle(ctx, toMessage(r)); err != nil {
					// не обработано и не в DLQ: отметка партиции не пройдёт это смещение — лаг растёт, это видно
					c.logger.ErrorContext(ctx, "kafka: сообщение не обработано, смещение не фиксируется", "partition", r.Partition, "offset", r.Offset, "err", err)
					return
				}
				if next, ok := c.tracker.finish(r); ok {
					c.cl.MarkCommitOffsets(map[string]map[int32]kgo.EpochOffset{
						r.Topic: {r.Partition: {Epoch: r.LeaderEpoch, Offset: next}},
					})
				}
			}(r)
		}
	}
}

// tracker считает водяную отметку по партициям: смещение, до которого всё обработано.
type tracker struct {
	mu       sync.Mutex
	inflight map[int32]map[int64]struct{}
	maxDone  map[int32]int64
}

func newTracker() *tracker {
	return &tracker{inflight: map[int32]map[int64]struct{}{}, maxDone: map[int32]int64{}}
}

func (t *tracker) start(r *kgo.Record) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.inflight[r.Partition] == nil {
		t.inflight[r.Partition] = map[int64]struct{}{}
	}
	t.inflight[r.Partition][r.Offset] = struct{}{}
}

// finish отмечает сообщение обработанным и возвращает новую отметку (следующее смещение к коммиту).
func (t *tracker) finish(r *kgo.Record) (int64, bool) {
	t.mu.Lock()
	defer t.mu.Unlock()
	delete(t.inflight[r.Partition], r.Offset)
	if r.Offset > t.maxDone[r.Partition] {
		t.maxDone[r.Partition] = r.Offset
	}
	low := int64(-1)
	for off := range t.inflight[r.Partition] {
		if low == -1 || off < low {
			low = off
		}
	}
	if low == -1 {
		return t.maxDone[r.Partition] + 1, true // всё начатое обработано
	}
	return low, true // коммит до первого незавершённого
}

func toMessage(r *kgo.Record) app.Message {
	h := make(map[string]string, len(r.Headers))
	for _, x := range r.Headers {
		h[x.Key] = string(x.Value)
	}
	return app.Message{Topic: r.Topic, Partition: r.Partition, Offset: r.Offset, Key: string(r.Key), Value: r.Value, Headers: h, Timestamp: r.Timestamp}
}

// Publish — app.DLQ: исходное сообщение с заголовками error, attempts, original_offset в топик DLQ.
func (c *Consumer) Publish(ctx context.Context, m app.Message, cause error, attempts int) error {
	rec := &kgo.Record{Topic: c.opts.DLQTopic, Key: []byte(m.Key), Value: m.Value}
	for k, v := range app.DLQHeaders(m, cause, attempts) {
		rec.Headers = append(rec.Headers, kgo.RecordHeader{Key: k, Value: []byte(v)})
	}
	return c.cl.ProduceSync(ctx, rec).FirstErr()
}

// Lag — отставание группы по топику (сообщений): сумма по партициям.
func (c *Consumer) Lag(ctx context.Context) (int64, error) {
	return groupLag(ctx, c.adm, c.opts.Group, c.opts.Topic)
}

// LagReader читает лаг группы без участия в ней: экспортёр лага работает и тогда, когда потребитель лежит
// (make kill-notifier: лаг растёт на дашборде, пока notifier остановлен).
type LagReader struct {
	cl           *kgo.Client
	adm          *kadm.Client
	group, topic string
}

// NewLagReader подключается к Kafka как административный клиент.
func NewLagReader(brokers []string, group, topic string) (*LagReader, error) {
	cl, err := kgo.NewClient(kgo.SeedBrokers(brokers...), kgo.ClientID("notifier-lag"))
	if err != nil {
		return nil, fmt.Errorf("kafka: клиент: %w", err)
	}
	return &LagReader{cl: cl, adm: kadm.NewClient(cl), group: group, topic: topic}, nil
}

// Lag — отставание группы по топику.
func (l *LagReader) Lag(ctx context.Context) (int64, error) {
	return groupLag(ctx, l.adm, l.group, l.topic)
}

// Ping — проверка соединения с брокером.
func (l *LagReader) Ping(ctx context.Context) error { return l.cl.Ping(ctx) }

// Close закрывает клиента.
func (l *LagReader) Close() { l.cl.Close() }

func groupLag(ctx context.Context, adm *kadm.Client, group, topic string) (int64, error) {
	lags, err := adm.Lag(ctx, group)
	if err != nil {
		return 0, err
	}
	l, ok := lags[group]
	if !ok {
		return 0, nil
	}
	if err := l.Error(); err != nil {
		return 0, err
	}
	return l.Lag.TotalByTopic()[topic].Lag, nil
}
