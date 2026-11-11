// Command notifier — сервис уведомлений (занятие 4): модуль notification, вынесенный из монолита (ADR-003).
//
// Источник событий OrderPaidEvent: Kafka (NOTIFIER_SOURCE=kafka, топик outbox.event.order, его наполняет Debezium)
// или polling таблицы outbox (NOTIFIER_SOURCE=outbox-poll, lite-режим без Kafka и Connect). Импортирует только
// notification, platform и booking/api (тип события) — правило в .go-arch-lint.yml.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"golang.org/x/sync/errgroup"

	notificationhttp "github.com/nikolaysavelev/soldout/internal/notification/adapters/http"
	notificationkafka "github.com/nikolaysavelev/soldout/internal/notification/adapters/kafka"
	notificationlog "github.com/nikolaysavelev/soldout/internal/notification/adapters/log"
	"github.com/nikolaysavelev/soldout/internal/notification/adapters/outboxpoll"
	notificationpg "github.com/nikolaysavelev/soldout/internal/notification/adapters/pg"
	"github.com/nikolaysavelev/soldout/internal/notification/app"
	"github.com/nikolaysavelev/soldout/internal/platform/db"
	platformlog "github.com/nikolaysavelev/soldout/internal/platform/log"
	"github.com/nikolaysavelev/soldout/internal/platform/otel"
	"github.com/nikolaysavelev/soldout/internal/platform/outbox"
)

var version = "dev"

type config struct {
	source      string // kafka | outbox-poll
	databaseURL string
	brokers     []string
	topic, dlq  string
	group       string
	maxAttempts int
	backoff     []time.Duration
	concurrency int
	mailgwURL   string
	addr        string
	otelURL     string
	logLevel    string
	logFormat   string
}

func main() {
	healthcheck := flag.Bool("healthcheck", false, "проверить /healthz работающего процесса и выйти")
	flag.Parse()
	cfg, err := fromEnv()
	if err != nil {
		fmt.Fprintln(os.Stderr, "notifier:", err)
		os.Exit(1)
	}
	if *healthcheck {
		os.Exit(runHealthcheck(cfg.addr))
	}
	if err := run(cfg); err != nil {
		fmt.Fprintln(os.Stderr, "notifier:", err)
		os.Exit(1)
	}
}

func fromEnv() (config, error) {
	c := config{
		source:      getenv("NOTIFIER_SOURCE", "kafka"),
		databaseURL: getenv("DATABASE_URL", "postgres://soldout:soldout@localhost:5432/soldout?sslmode=disable"),
		brokers:     strings.Split(getenv("KAFKA_BROKERS", "localhost:9092"), ","),
		topic:       getenv("NOTIFIER_TOPIC", "outbox.event.order"),
		dlq:         getenv("NOTIFIER_DLQ_TOPIC", "outbox.event.order.dlq"),
		group:       getenv("NOTIFIER_GROUP", "notifier"),
		mailgwURL:   os.Getenv("MAILGW_URL"),
		addr:        getenv("NOTIFIER_ADDR", ":8091"),
		otelURL:     os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT"),
		logLevel:    getenv("LOG_LEVEL", "info"),
		logFormat:   getenv("LOG_FORMAT", "text"),
	}
	if c.source != "kafka" && c.source != "outbox-poll" && c.source != "lag-exporter" {
		return c, fmt.Errorf("NOTIFIER_SOURCE=%q: ожидается kafka, outbox-poll или lag-exporter", c.source)
	}
	var err error
	if c.maxAttempts, err = strconv.Atoi(getenv("NOTIFIER_MAX_ATTEMPTS", "5")); err != nil || c.maxAttempts < 1 {
		return c, fmt.Errorf("NOTIFIER_MAX_ATTEMPTS: ожидается целое ≥ 1")
	}
	if c.concurrency, err = strconv.Atoi(getenv("NOTIFIER_CONCURRENCY", "256")); err != nil || c.concurrency < 1 {
		return c, fmt.Errorf("NOTIFIER_CONCURRENCY: ожидается целое ≥ 1")
	}
	for _, s := range strings.Split(getenv("NOTIFIER_BACKOFF", "500ms,1s,2s,4s"), ",") {
		d, err := time.ParseDuration(strings.TrimSpace(s))
		if err != nil {
			return c, fmt.Errorf("NOTIFIER_BACKOFF=%q: %w", s, err)
		}
		c.backoff = append(c.backoff, d)
	}
	return c, nil
}

func run(cfg config) error {
	logger := platformlog.New(cfg.logLevel, cfg.logFormat)
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	shutdownTracing, err := otel.Init(ctx, otel.Options{ServiceName: "notifier", ServiceVersion: version, Environment: "lab",
		Endpoint: cfg.otelURL, SampleRatio: 1})
	if err != nil {
		return err
	}
	defer func() {
		flushCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = shutdownTracing(flushCtx)
	}()

	pool, err := db.NewPool(ctx, cfg.databaseURL, db.PoolOptions{MaxConns: 16, MinConns: 2, AcquireTimeout: 5 * time.Second}, nil)
	if err != nil {
		return err
	}
	defer pool.Close()

	reg := prometheus.NewRegistry()
	reg.MustRegister(collectors.NewGoCollector(), collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))
	m := newMetrics(reg)

	var sender app.Sender = notificationlog.New(logger)
	if cfg.mailgwURL != "" {
		sender = notificationhttp.New(notificationhttp.Options{URL: cfg.mailgwURL, Timeout: 2 * time.Second, Retries: 1,
			Transport: otel.Transport(nil, "mailgw")})
	}
	store := notificationpg.New(pool)
	opts := app.Options{Consumer: "notifier", MaxAttempts: cfg.maxAttempts, Backoff: cfg.backoff}

	type source interface {
		Run(ctx context.Context, handle func(context.Context, app.Message) error) error
	}
	var (
		src   source
		proc  *app.Processor
		ready = func(context.Context) error { return nil }
		lagFn func(context.Context) (int64, error)
	)
	switch cfg.source {
	case "lag-exporter":
		// только метрика notifier_lag: отдельный процесс, чтобы лаг был виден, пока notifier остановлен
		lr, err := notificationkafka.NewLagReader(cfg.brokers, cfg.group, cfg.topic)
		if err != nil {
			return err
		}
		defer lr.Close()
		ready, lagFn = lr.Ping, lr.Lag
		src = idleSource{}
	case "kafka":
		kc, err := notificationkafka.New(notificationkafka.Options{Brokers: cfg.brokers, Topic: cfg.topic, DLQTopic: cfg.dlq,
			Group: cfg.group, Concurrency: cfg.concurrency}, logger)
		if err != nil {
			return err
		}
		defer kc.Close()
		src, ready, lagFn = kc, kc.Ping, kc.Lag
		proc = app.New(store, sender, kc, m, opts, logger)
	case "outbox-poll":
		src = outboxpoll.New(outbox.NewPollerFor("order", pool, 200, 200*time.Millisecond), cfg.concurrency)
		dlq := outboxpoll.LogDLQ{Log: func(ctx context.Context, msg app.Message, cause error, attempts int) {
			logger.ErrorContext(ctx, "notifier: DLQ (lite, без брокера)", "headers", app.DLQHeaders(msg, cause, attempts), "value", string(msg.Value))
		}}
		lagFn = func(ctx context.Context) (int64, error) {
			var n int64
			err := pool.QueryRow(ctx, `/* notifier.outbox_backlog */ SELECT count(*) FROM outbox`).Scan(&n)
			return n, err
		}
		proc = app.New(store, sender, dlq, m, opts, logger)
	}

	mux := http.NewServeMux()
	mux.Handle("GET /metrics", promhttp.HandlerFor(reg, promhttp.HandlerOpts{}))
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) {
		if err := ready(r.Context()); err != nil {
			http.Error(w, err.Error(), http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusOK)
	})
	srv := &http.Server{Addr: cfg.addr, Handler: mux, ReadHeaderTimeout: 5 * time.Second}

	logger.Info("старт notifier", "source", cfg.source, "topic", cfg.topic, "group", cfg.group, "max_attempts", cfg.maxAttempts,
		"backoff", fmt.Sprint(cfg.backoff), "concurrency", cfg.concurrency, "mailgw", cfg.mailgwURL)
	g, gctx := errgroup.WithContext(ctx)
	g.Go(func() error {
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			return err
		}
		return nil
	})
	g.Go(func() error {
		var handle func(context.Context, app.Message) error
		if proc != nil {
			handle = proc.Handle
		}
		return src.Run(gctx, handle)
	})
	g.Go(func() error { // лаг потребителя раз в 5 с: главный сигнал «notifier не успевает» (и для HPA на занятии 5)
		t := time.NewTicker(5 * time.Second)
		defer t.Stop()
		for {
			select {
			case <-gctx.Done():
				return nil
			case <-t.C:
				if lag, err := lagFn(gctx); err == nil {
					m.lag.Set(float64(lag))
				} else if gctx.Err() == nil {
					logger.Warn("notifier: лаг не получен", "err", err)
				}
			}
		}
	})
	g.Go(func() error {
		<-gctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		return srv.Shutdown(shutdownCtx)
	})
	err = g.Wait()
	logger.Info("notifier остановлен")
	return err
}

// idleSource — источник экспортёра лага: ничего не читает, ждёт остановки.
type idleSource struct{}

func (idleSource) Run(ctx context.Context, _ func(context.Context, app.Message) error) error {
	<-ctx.Done()
	return nil
}

type metrics struct {
	consumed, duplicate, failed, dlq prometheus.Counter
	delay                            prometheus.Histogram
	lag                              prometheus.Gauge
}

func newMetrics(reg *prometheus.Registry) *metrics {
	m := &metrics{
		consumed:  prometheus.NewCounter(prometheus.CounterOpts{Name: "notifier_consumed_total", Help: "Сообщений принято в обработку."}),
		duplicate: prometheus.NewCounter(prometheus.CounterOpts{Name: "notifier_duplicates_total", Help: "Повторных доставок уже отправленного письма (at-least-once)."}),
		failed:    prometheus.NewCounter(prometheus.CounterOpts{Name: "notifier_failed_total", Help: "Неудачных попыток обработки (каждая попытка)."}),
		dlq:       prometheus.NewCounter(prometheus.CounterOpts{Name: "notifier_dlq_total", Help: "Сообщений отправлено в DLQ после всех попыток."}),
		delay: prometheus.NewHistogram(prometheus.HistogramOpts{Name: "notifier_delivery_delay_seconds",
			Help:    "От commit оплаты (OrderPaidEvent.at) до отправленного письма.",
			Buckets: []float64{.25, .5, .75, 1, 1.5, 2, 3, 5, 10, 30, 60}}),
		lag: prometheus.NewGauge(prometheus.GaugeOpts{Name: "notifier_lag", Help: "Отставание потребителя: сообщений в топике после зафиксированного смещения (lite — строк в outbox)."}),
	}
	reg.MustRegister(m.consumed, m.duplicate, m.failed, m.dlq, m.delay, m.lag)
	return m
}

func (m *metrics) Consumed()                     { m.consumed.Inc() }
func (m *metrics) Duplicate()                    { m.duplicate.Inc() }
func (m *metrics) Failed()                       { m.failed.Inc() }
func (m *metrics) DeadLettered()                 { m.dlq.Inc() }
func (m *metrics) DeliveryDelay(d time.Duration) { m.delay.Observe(d.Seconds()) }

func getenv(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

func runHealthcheck(addr string) int {
	if addr[0] == ':' {
		addr = "127.0.0.1" + addr
	}
	client := &http.Client{Timeout: 3 * time.Second}
	resp, err := client.Get("http://" + addr + "/healthz")
	if err != nil {
		return 1
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return 1
	}
	return 0
}
