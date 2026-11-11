package outbox_test

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/testcontainers/testcontainers-go"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"
	gootel "go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"

	"github.com/nikolaysavelev/soldout/internal/platform/db"
	"github.com/nikolaysavelev/soldout/internal/platform/otel"
	"github.com/nikolaysavelev/soldout/internal/platform/outbox"
)

type testEvent struct {
	ID        uuid.UUID `json:"id"`
	OrderID   uuid.UUID `json:"order_id"`
	Aggregate string    `json:"-"`
	Type      string    `json:"-"`
}

func (e testEvent) OutboxID() uuid.UUID { return e.ID }
func (e testEvent) AggregateType() string {
	if e.Aggregate != "" {
		return e.Aggregate
	}
	return "order"
}
func (e testEvent) AggregateID() string { return e.OrderID.String() }
func (e testEvent) EventType() string {
	if e.Type != "" {
		return e.Type
	}
	return "OrderPaid"
}

func startDB(t *testing.T) *db.Pool {
	t.Helper()
	if testing.Short() {
		t.Skip("интеграционный тест: нужен Docker (пропуск с -short)")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	t.Cleanup(cancel)
	pgc, err := tcpostgres.Run(ctx, "postgres:17-alpine",
		tcpostgres.WithDatabase("soldout"), tcpostgres.WithUsername("soldout"), tcpostgres.WithPassword("soldout"),
		testcontainers.WithWaitStrategy(wait.ForLog("database system is ready to accept connections").WithOccurrence(2).WithStartupTimeout(60*time.Second)),
	)
	if err != nil {
		t.Fatalf("postgres container: %v", err)
	}
	t.Cleanup(func() { _ = pgc.Terminate(context.Background()) })
	url, err := pgc.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Migrate(ctx, url, slog.Default()); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	pool, err := db.NewPool(ctx, url, db.PoolOptions{MaxConns: 5}, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	if _, err := pool.Exec(ctx, `ALTER TABLE outbox ADD COLUMN IF NOT EXISTS seq bigint GENERATED ALWAYS AS IDENTITY`); err != nil {
		t.Fatalf("outbox seq: %v", err)
	}
	return pool
}

func count(t *testing.T, pool *db.Pool, id uuid.UUID) (n int, traceparent string) {
	t.Helper()
	err := pool.QueryRow(context.Background(), `SELECT count(*), coalesce(max(traceparent), '') FROM outbox WHERE id = $1`, id).Scan(&n, &traceparent)
	if err != nil {
		t.Fatal(err)
	}
	return n, traceparent
}

// Строка outbox пишется в транзакции издателя: rollback отменяет событие вместе с изменением,
// commit делает его видимым; traceparent заполнен, если в ctx есть span. Режим CDC: таблица пуста после commit.
func TestOutboxAppend(t *testing.T) {
	pool := startDB(t)
	ctx := context.Background()

	tp := sdktrace.NewTracerProvider()
	gootel.SetTracerProvider(tp)
	gootel.SetTextMapPropagator(propagation.TraceContext{})
	spanCtx, span := otel.Start(ctx, "booking.PayOrder")
	defer span.End()

	keep := outbox.NewWriter(outbox.Options{DeleteAfterInsert: false})
	errBoom := errors.New("rollback")

	t.Run("rollback отменяет событие", func(t *testing.T) {
		e := testEvent{ID: uuid.New(), OrderID: uuid.New()}
		err := db.WithTx(ctx, pool, func(tx pgx.Tx) error {
			if err := keep.Append(spanCtx, tx, e); err != nil {
				return err
			}
			return errBoom
		})
		if !errors.Is(err, errBoom) {
			t.Fatalf("ожидали ошибку транзакции, получили %v", err)
		}
		if n, _ := count(t, pool, e.ID); n != 0 {
			t.Fatalf("после rollback строка outbox видна: %d", n)
		}
	})

	t.Run("commit публикует, traceparent из span", func(t *testing.T) {
		e := testEvent{ID: uuid.New(), OrderID: uuid.New()}
		if err := db.WithTx(ctx, pool, func(tx pgx.Tx) error { return keep.Append(spanCtx, tx, e) }); err != nil {
			t.Fatal(err)
		}
		n, traceparent := count(t, pool, e.ID)
		if n != 1 {
			t.Fatalf("после commit ожидали 1 строку, получили %d", n)
		}
		if want := "00-" + span.SpanContext().TraceID().String(); len(traceparent) < len(want) || traceparent[:len(want)] != want {
			t.Fatalf("traceparent %q не из текущего трейса %s", traceparent, span.SpanContext().TraceID())
		}
	})

	t.Run("без span traceparent пуст", func(t *testing.T) {
		e := testEvent{ID: uuid.New(), OrderID: uuid.New()}
		if err := db.WithTx(ctx, pool, func(tx pgx.Tx) error { return keep.Append(ctx, tx, e) }); err != nil {
			t.Fatal(err)
		}
		if _, traceparent := count(t, pool, e.ID); traceparent != "" {
			t.Fatalf("traceparent без span: %q", traceparent)
		}
	})

	t.Run("CDC: строка удаляется в той же транзакции", func(t *testing.T) {
		e := testEvent{ID: uuid.New(), OrderID: uuid.New()}
		cdc := outbox.NewWriter(outbox.Options{DeleteAfterInsert: true})
		if err := db.WithTx(ctx, pool, func(tx pgx.Tx) error { return cdc.Append(spanCtx, tx, e) }); err != nil {
			t.Fatal(err)
		}
		if n, _ := count(t, pool, e.ID); n != 0 {
			t.Fatalf("в режиме CDC таблица должна быть пуста, строк: %d", n)
		}
	})

	t.Run("poller отдаёт и удаляет строки", func(t *testing.T) {
		poller := outbox.NewPoller(pool, 100, 10*time.Millisecond)
		// отмена посреди пачки не даёт ack: пачка придёт снова (at-least-once), поэтому здесь просто таймаут
		pctx, cancel := context.WithTimeout(ctx, 500*time.Millisecond)
		defer cancel()
		var got int
		_ = poller.Run(pctx, func(_ context.Context, batch []outbox.Record) error {
			got += len(batch)
			return nil
		})
		if got != 2 { // две строки из подтестов без удаления
			t.Fatalf("poller отдал %d строк, ожидали 2", got)
		}
		var left int
		if err := pool.QueryRow(ctx, `SELECT count(*) FROM outbox`).Scan(&left); err != nil {
			t.Fatal(err)
		}
		if left != 0 {
			t.Fatalf("после ack в outbox осталось %d строк", left)
		}
	})
}

func TestOutboxAppendBatch(t *testing.T) {
	pool := startDB(t)
	ctx := context.Background()
	keep := outbox.NewWriter(outbox.Options{DeleteAfterInsert: false})

	t.Run("batch insert сохраняет события и payload", func(t *testing.T) {
		events := []outbox.Event{
			testEvent{ID: uuid.New(), OrderID: uuid.New()},
			testEvent{ID: uuid.New(), OrderID: uuid.New(), Aggregate: "ticket", Type: "TicketIssued"},
			testEvent{ID: uuid.New(), OrderID: uuid.New()},
		}
		if err := db.WithTx(ctx, pool, func(tx pgx.Tx) error { return keep.AppendBatch(ctx, tx, events) }); err != nil {
			t.Fatal(err)
		}
		rows, err := pool.Query(ctx, `SELECT id, payload, seq FROM outbox ORDER BY seq`)
		if err != nil {
			t.Fatal(err)
		}
		defer rows.Close()
		var previousSeq int64
		for i := 0; rows.Next(); i++ {
			if i >= len(events) {
				t.Fatal("batch insert создал лишние строки")
			}
			var id uuid.UUID
			var payload []byte
			var seq int64
			if err := rows.Scan(&id, &payload, &seq); err != nil {
				t.Fatal(err)
			}
			if id != events[i].OutboxID() {
				t.Fatalf("строка %d: id %s, ожидали %s", i, id, events[i].OutboxID())
			}
			if seq <= previousSeq {
				t.Fatalf("seq не возрастает: %d после %d", seq, previousSeq)
			}
			previousSeq = seq
			var got testEvent
			if err := json.Unmarshal(payload, &got); err != nil {
				t.Fatal(err)
			}
			if got.ID != events[i].OutboxID() {
				t.Fatalf("строка %d: payload id %s, ожидали %s", i, got.ID, events[i].OutboxID())
			}
		}
		if err := rows.Err(); err != nil {
			t.Fatal(err)
		}
	})

	t.Run("пустой batch не меняет таблицу", func(t *testing.T) {
		if err := db.WithTx(ctx, pool, func(tx pgx.Tx) error { return keep.AppendBatch(ctx, tx, nil) }); err != nil {
			t.Fatal(err)
		}
		var n int
		if err := pool.QueryRow(ctx, `SELECT count(*) FROM outbox`).Scan(&n); err != nil {
			t.Fatal(err)
		}
		if n != 3 {
			t.Fatalf("после пустого batch строк: %d, ожидали 3", n)
		}
	})

	t.Run("rollback отменяет весь batch", func(t *testing.T) {
		events := []outbox.Event{
			testEvent{ID: uuid.New(), OrderID: uuid.New()},
			testEvent{ID: uuid.New(), OrderID: uuid.New()},
		}
		errBoom := errors.New("rollback batch")
		err := db.WithTx(ctx, pool, func(tx pgx.Tx) error {
			if err := keep.AppendBatch(ctx, tx, events); err != nil {
				return err
			}
			return errBoom
		})
		if !errors.Is(err, errBoom) {
			t.Fatalf("ожидали ошибку транзакции, получили %v", err)
		}
		for _, e := range events {
			if n, _ := count(t, pool, e.OutboxID()); n != 0 {
				t.Fatalf("после rollback событие %s видно", e.OutboxID())
			}
		}
	})

	t.Run("CDC удаляет весь batch одной операцией записи", func(t *testing.T) {
		events := []outbox.Event{
			testEvent{ID: uuid.New(), OrderID: uuid.New()},
			testEvent{ID: uuid.New(), OrderID: uuid.New()},
		}
		cdc := outbox.NewWriter(outbox.Options{DeleteAfterInsert: true})
		if err := db.WithTx(ctx, pool, func(tx pgx.Tx) error { return cdc.AppendBatch(ctx, tx, events) }); err != nil {
			t.Fatal(err)
		}
		for _, e := range events {
			if n, _ := count(t, pool, e.OutboxID()); n != 0 {
				t.Fatalf("в режиме CDC событие %s осталось в таблице", e.OutboxID())
			}
		}
	})
}

func pollRecords(t *testing.T, poller *outbox.Poller) []outbox.Record {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 150*time.Millisecond)
	defer cancel()
	var got []outbox.Record
	if err := poller.Run(ctx, func(_ context.Context, batch []outbox.Record) error {
		got = append(got, batch...)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	return got
}

func TestOutboxPollerFilterAndSeq(t *testing.T) {
	pool := startDB(t)
	ctx := context.Background()
	writer := outbox.NewWriter(outbox.Options{DeleteAfterInsert: false})

	t.Run("filter consumer не получает и не ack чужой тип", func(t *testing.T) {
		events := []outbox.Event{
			testEvent{ID: uuid.New(), OrderID: uuid.New()},
			testEvent{ID: uuid.New(), OrderID: uuid.New(), Aggregate: "ticket", Type: "TicketIssued"},
			testEvent{ID: uuid.New(), OrderID: uuid.New()},
		}
		if err := db.WithTx(ctx, pool, func(tx pgx.Tx) error { return writer.AppendBatch(ctx, tx, events) }); err != nil {
			t.Fatal(err)
		}
		got := pollRecords(t, outbox.NewPollerFor("order", pool, 10, 5*time.Millisecond))
		if len(got) != 2 {
			t.Fatalf("filter consumer получил %d событий, ожидали 2", len(got))
		}
		for i, r := range got {
			if r.AggregateType != "order" || r.ID != events[i*2].OutboxID() {
				t.Fatalf("неожиданная запись filter consumer: %+v", r)
			}
			if r.Seq == 0 || (i > 0 && r.Seq <= got[i-1].Seq) {
				t.Fatalf("неверный порядок seq: %+v", got)
			}
		}
		if n, _ := count(t, pool, events[1].OutboxID()); n != 1 {
			t.Fatalf("filter consumer ack чужого события: строк %d", n)
		}
		got = pollRecords(t, outbox.NewPoller(pool, 10, 5*time.Millisecond))
		if len(got) != 1 || got[0].ID != events[1].OutboxID() {
			t.Fatalf("общий poller не получил оставшееся событие: %+v", got)
		}
	})

	t.Run("poller отдаёт batch в порядке seq", func(t *testing.T) {
		events := []outbox.Event{
			testEvent{ID: uuid.New(), OrderID: uuid.New()},
			testEvent{ID: uuid.New(), OrderID: uuid.New()},
			testEvent{ID: uuid.New(), OrderID: uuid.New()},
		}
		if err := db.WithTx(ctx, pool, func(tx pgx.Tx) error { return writer.AppendBatch(ctx, tx, events) }); err != nil {
			t.Fatal(err)
		}
		got := pollRecords(t, outbox.NewPoller(pool, 2, 5*time.Millisecond))
		if len(got) != len(events) {
			t.Fatalf("poller получил %d событий, ожидали %d", len(got), len(events))
		}
		for i, r := range got {
			if r.ID != events[i].OutboxID() {
				t.Fatalf("позиция %d: id %s, ожидали %s", i, r.ID, events[i].OutboxID())
			}
			if r.Seq == 0 || (i > 0 && r.Seq <= got[i-1].Seq) {
				t.Fatalf("неверный порядок seq: %+v", got)
			}
		}
	})
}
