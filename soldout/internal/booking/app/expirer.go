package app

import (
	"context"
	"log/slog"
	"time"
)

// Gauge — приёмник значения holds_active (metrics.HoldsActive).
type Gauge interface{ Set(float64) }

// Observer — приёмник размера батча (metrics.ExpirerBatchSize).
type Observer interface{ Observe(float64) }

// Expirer раз в interval переводит просроченные active hold в released батчами по BatchSize
// (`… LIMIT n FOR UPDATE SKIP LOCKED`) с паузой между батчами — один большой UPDATE под штормом
// давал всплеск p99 ровно в момент замера. Истекает заказы с такими hold и обновляет holds_active.
// На занятии 7 заменяется таймером Temporal.
type Expirer struct {
	store     Store
	interval  time.Duration
	batchSize int
	pause     time.Duration
	gauge     Gauge
	batches   Observer
	logger    *slog.Logger
}

// ExpirerOptions — настройки.
type ExpirerOptions struct {
	Interval  time.Duration
	BatchSize int
	Pause     time.Duration
	Gauge     Gauge
	Batches   Observer
}

// NewExpirer собирает expirer.
func NewExpirer(store Store, opts ExpirerOptions, logger *slog.Logger) *Expirer {
	if opts.Interval <= 0 {
		opts.Interval = 5 * time.Second
	}
	if opts.BatchSize <= 0 {
		opts.BatchSize = 500
	}
	if opts.Pause <= 0 {
		opts.Pause = 50 * time.Millisecond
	}
	return &Expirer{store: store, interval: opts.Interval, batchSize: opts.BatchSize, pause: opts.Pause,
		gauge: opts.Gauge, batches: opts.Batches, logger: logger}
}

// Run блокируется до отмены ctx.
func (e *Expirer) Run(ctx context.Context) {
	t := time.NewTicker(e.interval)
	defer t.Stop()
	for {
		e.tick(ctx)
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
	}
}

func (e *Expirer) tick(ctx context.Context) {
	ctx, cancel := context.WithTimeout(ctx, e.interval)
	defer cancel()
	var total int
	for {
		released, err := e.store.ExpireHoldsBatch(ctx, time.Now(), e.batchSize)
		if err != nil {
			e.logger.ErrorContext(ctx, "expirer: hold", "err", err)
			return
		}
		if e.batches != nil {
			e.batches.Observe(float64(len(released)))
		}
		total += len(released)
		if len(released) < e.batchSize {
			break
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(e.pause):
		}
	}
	orders, err := e.store.ExpireOrders(ctx, time.Now())
	if err != nil {
		e.logger.ErrorContext(ctx, "expirer: orders", "err", err)
		return
	}
	if total > 0 || orders > 0 {
		e.logger.InfoContext(ctx, "expirer", "holds_released", total, "orders_expired", orders)
	}
	if e.gauge != nil {
		if n, err := e.store.CountActiveHolds(ctx); err == nil {
			e.gauge.Set(float64(n))
		}
	}
}
