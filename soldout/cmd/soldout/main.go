// Command soldout — модульный монолит «билетного дропа»: сборка зависимостей, HTTP-сервер, graceful shutdown.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"net/http"
	"net/http/pprof"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	"golang.org/x/sync/errgroup"

	bookinghttp "github.com/nikolaysavelev/soldout/internal/booking/adapters/http"
	bookingpg "github.com/nikolaysavelev/soldout/internal/booking/adapters/pg"
	bookingvalkey "github.com/nikolaysavelev/soldout/internal/booking/adapters/valkey"
	bookingapi "github.com/nikolaysavelev/soldout/internal/booking/api"
	bookingapp "github.com/nikolaysavelev/soldout/internal/booking/app"
	cataloghttp "github.com/nikolaysavelev/soldout/internal/catalog/adapters/http"
	catalogpg "github.com/nikolaysavelev/soldout/internal/catalog/adapters/pg"
	catalogvalkey "github.com/nikolaysavelev/soldout/internal/catalog/adapters/valkey"
	catalogapp "github.com/nikolaysavelev/soldout/internal/catalog/app"
	paymentpg "github.com/nikolaysavelev/soldout/internal/payment/adapters/pg"
	paymentpsp "github.com/nikolaysavelev/soldout/internal/payment/adapters/psp"
	paymentapp "github.com/nikolaysavelev/soldout/internal/payment/app"
	"github.com/nikolaysavelev/soldout/internal/platform/cache"
	"github.com/nikolaysavelev/soldout/internal/platform/config"
	"github.com/nikolaysavelev/soldout/internal/platform/db"
	"github.com/nikolaysavelev/soldout/internal/platform/eventbus"
	"github.com/nikolaysavelev/soldout/internal/platform/faults"
	"github.com/nikolaysavelev/soldout/internal/platform/httpx"
	platformlog "github.com/nikolaysavelev/soldout/internal/platform/log"
	"github.com/nikolaysavelev/soldout/internal/platform/metrics"
	"github.com/nikolaysavelev/soldout/internal/platform/otel"
	"github.com/nikolaysavelev/soldout/internal/platform/outbox"
	queuehttp "github.com/nikolaysavelev/soldout/internal/queue/adapters/http"
	queuepg "github.com/nikolaysavelev/soldout/internal/queue/adapters/pg"
	queuevalkey "github.com/nikolaysavelev/soldout/internal/queue/adapters/valkey"
	queueapp "github.com/nikolaysavelev/soldout/internal/queue/app"
	ticketinghttp "github.com/nikolaysavelev/soldout/internal/ticketing/adapters/http"
	ticketingpg "github.com/nikolaysavelev/soldout/internal/ticketing/adapters/pg"
	ticketingapp "github.com/nikolaysavelev/soldout/internal/ticketing/app"
)

// version задаётся при сборке: -ldflags "-X main.version=..." (service.version в трейсах).
var version = "dev"

func main() {
	healthcheck := flag.Bool("healthcheck", false, "проверить /readyz работающего процесса и выйти (для HEALTHCHECK в distroless)")
	flag.Parse()
	if *healthcheck {
		os.Exit(runHealthcheck())
	}
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "soldout:", err)
		os.Exit(1)
	}
}

func run() error {
	cfg, err := config.FromEnv()
	if err != nil {
		return err
	}
	flags, err := faults.FromEnv()
	if err != nil {
		return err
	}
	logger := platformlog.New(cfg.LogLevel, cfg.LogFormat)
	logger.Info("старт soldout", "addr", cfg.HTTPAddr, "hold_ttl", cfg.HoldTTL.String(), "db_max_conns", cfg.DBMaxConns,
		"db_acquire_timeout", cfg.DBAcquireTimeout.String(), "psp_fail_rate", flags.PSPFailRate, "psp_delay", flags.SlowPSP.String(),
		"otel_endpoint", cfg.OTelEndpoint, "otel_sample_ratio", cfg.OTelSampleRatio, "otel_noisy_ratio", cfg.OTelNoisyRatio,
		"outbox_delete_after_insert", cfg.OutboxDelete,
		"gomaxprocs", httpx.RuntimeDiagnostics()["gomaxprocs"], "gomemlimit", httpx.RuntimeDiagnostics()["gomemlimit"])

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	// --- трейсинг (занятие 4): OTLP → otel-collector → Tempo; flush при остановке ---
	shutdownTracing, err := otel.Init(ctx, otel.Options{
		ServiceName: "soldout", ServiceVersion: version, Environment: cfg.Environment,
		Endpoint: cfg.OTelEndpoint, SampleRatio: cfg.OTelSampleRatio,
		NoisyRatio: cfg.OTelNoisyRatio, NoisyRoutes: []string{"POST /v1/holds", "POST /v1/events/", "GET /v1/events/"},
	})
	if err != nil {
		return err
	}
	defer func() {
		flushCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = shutdownTracing(flushCtx)
	}()

	// --- платформа ---
	m := metrics.New()
	if cfg.MigrateOnStart {
		// миграции и seed — напрямую в PostgreSQL: golang-migrate держит сессионный advisory-lock,
		// который несовместим с transaction pooling PgBouncer
		if err := db.Migrate(ctx, cfg.DatabaseURLDirect, logger); err != nil {
			return err
		}
	}
	if cfg.SeedOnStart {
		direct, err := db.NewPool(ctx, cfg.DatabaseURLDirect, db.PoolOptions{MaxConns: 2, AcquireTimeout: 10 * time.Second}, nil)
		if err != nil {
			return err
		}
		err = db.Seed(ctx, direct, cfg.SeedSeatsPerEvent, logger)
		direct.Close()
		if err != nil {
			return err
		}
	}
	pool, err := db.NewPool(ctx, cfg.DatabaseURL, db.PoolOptions{
		MaxConns: cfg.DBMaxConns, MinConns: cfg.DBMinConns, MaxConnIdleTime: cfg.DBMaxConnIdleTime, AcquireTimeout: cfg.DBAcquireTimeout,
	}, m)
	if err != nil {
		return err
	}
	defer pool.Close()
	vk, err := cache.New(ctx, cfg.ValkeyAddr)
	if err != nil {
		return err
	}
	defer vk.Close()

	// --- модули: сборка по правилу зависимостей (см. .go-arch-lint.yml) ---
	bus := eventbus.New(cfg.EventbusWorkers, cfg.EventbusBuffer, m, logger)
	// OrderPaidEvent → outbox в транзакции pay; доставка — Debezium (CDC) или polling notifier'а (lite)
	bookingStore := bookingpg.New(pool, bookingpg.WithOutbox(outbox.NewWriter(outbox.Options{DeleteAfterInsert: cfg.OutboxDelete})))
	// карта зала — проекция в catalog, наполняется из событий booking; кэш — только проекция с версией из БД
	catalogSvc := catalogapp.New(catalogpg.New(pool), catalogvalkey.New(vk),
		catalogapp.Options{CacheTTL: cfg.CacheTTL, CacheInvalidate: cfg.CacheInvalidate}, cacheOps{m}, logger)
	bus.Subscribe(bookingapi.TopicSeatState, catalogSvc.HandleSeatState) // подписка — в cmd, модули друг о друге не знают
	queueSvc := queueapp.New(queuepg.New(pool), queuevalkey.New(vk), catalogSvc, cfg.AdmissionTTL, logger)
	paymentSvc := paymentapp.New(paymentpsp.New(flags.PSPFailRate, flags.SlowPSP), paymentpg.New(pool))
	// notification с занятия 4 — отдельный сервис cmd/notifier (событие OrderPaidEvent), в монолите его нет
	ticketingSvc := ticketingapp.New(ticketingpg.New(pool), logger)
	bookingSvc := bookingapp.New(bookingStore, catalogSvc, queueSvc, paymentSvc, ticketingSvc,
		bookingapp.Options{HoldTTL: cfg.HoldTTL, Pricing: bookingvalkey.New(vk), Faults: flags, Events: bus}, logger)

	// --- HTTP ---
	r := chi.NewRouter()
	r.Use(middleware.RequestID, middleware.RealIP, otel.NameByRoute, httpx.WithLogger(logger), httpx.Recover)
	r.Get("/healthz", httpx.Healthz)
	r.Get("/readyz", httpx.Readyz(map[string]httpx.Checker{
		"postgres": pool.Ping,
		"valkey":   func(ctx context.Context) error { return cache.Ping(ctx, vk) },
	}, httpx.RuntimeDiagnostics(), httpx.Diagnostics{
		"db_max_conns": cfg.DBMaxConns, "db_acquire_timeout": cfg.DBAcquireTimeout.String(),
		"pgbouncer": db.PgBouncerVersion(ctx, cfg.DatabaseURL),
	}))
	r.Handle("/metrics", m.Handler())
	r.Route("/debug/pprof", func(r chi.Router) {
		r.Use(httpx.ExtendWriteDeadline(90 * time.Second)) // профиль на 20–30 с длиннее WriteTimeout сервера
		r.Get("/", pprof.Index)
		r.Get("/cmdline", pprof.Cmdline)
		r.Get("/profile", pprof.Profile)
		r.Get("/symbol", pprof.Symbol)
		r.Get("/trace", pprof.Trace)
		r.Get("/{name}", func(w http.ResponseWriter, req *http.Request) {
			pprof.Handler(chi.URLParam(req, "name")).ServeHTTP(w, req)
		})
	})
	r.Group(func(r chi.Router) {
		r.Use(httpx.Instrument(m), httpx.Timeout(cfg.HandlerTimeout)) // 3 с < WriteTimeout 15 с
		cataloghttp.New(catalogSvc).Mount(r)
		queuehttp.New(queueSvc).Mount(r)
		bookinghttp.New(bookingSvc).Mount(r)
		ticketinghttp.New(ticketingSvc, bookingSvc).Mount(r)
	})

	srv := &http.Server{
		Addr: cfg.HTTPAddr, Handler: otel.Handler(r, "soldout"),
		ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 10 * time.Second, WriteTimeout: 15 * time.Second,
		IdleTimeout: 60 * time.Second, MaxHeaderBytes: 64 << 10,
	}

	// --- фоновые процессы и порядок остановки: стоп приёма → Shutdown → отмена expirer ---
	bgCtx, stopBackground := context.WithCancel(context.Background())
	defer stopBackground()
	g, gctx := errgroup.WithContext(ctx)
	busCtx, stopBus := context.WithCancel(context.Background())
	defer stopBus()
	g.Go(func() error { bus.Run(busCtx); return nil })
	g.Go(func() error {
		bookingapp.NewExpirer(bookingStore, bookingapp.ExpirerOptions{
			Interval: cfg.ExpirerInterval, BatchSize: cfg.ExpirerBatch, Gauge: m.HoldsActive, Batches: m.ExpirerBatchSize,
			OnRelease: func(ctx context.Context, released []bookingapp.ReleasedHold) {
				for _, h := range released {
					_ = bus.Publish(ctx, bookingapi.SeatStateChanged{EventID: h.EventID, SeatID: h.SeatID, State: bookingapi.SeatStateFree, HoldID: h.ID, At: time.Now()})
				}
			},
		}, logger).Run(bgCtx)
		return nil
	})
	g.Go(func() error {
		logger.Info("HTTP-сервер слушает", "addr", cfg.HTTPAddr)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			return err
		}
		return nil
	})
	g.Go(func() error {
		<-gctx.Done() // сигнал или падение сервера
		logger.Info("остановка: ждём завершения запросов", "timeout", cfg.ShutdownTimeout.String())
		shutdownCtx, cancel := context.WithTimeout(context.Background(), cfg.ShutdownTimeout)
		defer cancel()
		if err := srv.Shutdown(shutdownCtx); err != nil {
			logger.Warn("HTTP-сервер остановлен принудительно", "err", err)
		}
		stopBackground() // expirer
		stopBus()        // шина дочитывает буфер (drain) и завершает воркеры
		return nil
	})
	if err := g.Wait(); err != nil {
		return err
	}
	logger.Info("soldout остановлен")
	return nil
}

// cacheOps — адаптер счётчика операций кэша для catalog.
type cacheOps struct{ m *metrics.Metrics }

func (c cacheOps) Inc(op string) { c.m.CacheOps.WithLabelValues(op).Inc() }

func runHealthcheck() int {
	addr := os.Getenv("HTTP_ADDR")
	if addr == "" {
		addr = ":8080"
	}
	if addr[0] == ':' {
		addr = "127.0.0.1" + addr
	}
	client := &http.Client{Timeout: 3 * time.Second}
	resp, err := client.Get("http://" + addr + "/readyz")
	if err != nil {
		fmt.Fprintln(os.Stderr, "healthcheck:", err)
		return 1
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		fmt.Fprintln(os.Stderr, "healthcheck: status", resp.StatusCode)
		return 1
	}
	return 0
}
