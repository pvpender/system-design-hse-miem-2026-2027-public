package app

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/testcontainers/testcontainers-go"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"

	bookingapi "github.com/nikolaysavelev/soldout/internal/booking/api"
	catalogpg "github.com/nikolaysavelev/soldout/internal/catalog/adapters/pg"
	"github.com/nikolaysavelev/soldout/internal/catalog/api"
	"github.com/nikolaysavelev/soldout/internal/catalog/domain"
	"github.com/nikolaysavelev/soldout/internal/platform/db"
	"github.com/nikolaysavelev/soldout/internal/platform/outbox"
)

func TestSeatStateBatch_IdempotentAndOrdered(t *testing.T) {
	if testing.Short() {
		t.Skip("интеграционный тест: нужен Docker (пропуск с -short)")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	pgc, err := tcpostgres.Run(ctx, "postgres:17-alpine",
		tcpostgres.WithDatabase("soldout"), tcpostgres.WithUsername("soldout"), tcpostgres.WithPassword("soldout"),
		testcontainers.WithWaitStrategy(wait.ForLog("database system is ready to accept connections").WithOccurrence(2).WithStartupTimeout(60*time.Second)),
	)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = pgc.Terminate(context.Background()) })
	url, err := pgc.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Migrate(ctx, url, slog.Default()); err != nil {
		t.Fatal(err)
	}
	pool, err := db.NewPool(ctx, url, db.PoolOptions{}, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	if err := db.Seed(ctx, pool, 3000, nil); err != nil {
		t.Fatal(err)
	}
	cache := &projectorCache{}
	svc := New(catalogpg.New(pool), cache, Options{CacheInvalidate: true}, nil, slog.Default())
	eventID := uuid.MustParse("10000000-0000-4000-8000-000000000001")
	sectorID := uuid.MustParse("30000000-0000-4000-8000-0000000003e9")
	_, before, err := svc.repo.SectorStates(ctx, eventID, sectorID)
	if err != nil {
		t.Fatal(err)
	}
	batch := []outbox.Record{
		seatRecord(t, 1, bookingapi.SeatStateHeld, 1),
		seatRecord(t, 2, bookingapi.SeatStateFree, 1),
		seatRecord(t, 3, bookingapi.SeatStateSold, 2),
	}
	if err := svc.HandleSeatStateBatch(ctx, batch); err != nil {
		t.Fatal(err)
	}
	states, appliedVersion, err := svc.repo.SectorStates(ctx, eventID, sectorID)
	var processed, positions int
	_ = pool.QueryRow(ctx, "SELECT (SELECT count(*) FROM catalog_processed_events), (SELECT count(*) FROM catalog_seat_projection_position)").Scan(&processed, &positions)
	t.Logf("processed=%d positions=%d version=%d", processed, positions, appliedVersion)
	if err != nil || len(states) != 1 || states[0].SeatID != 2 || states[0].Status != api.SeatSold {
		t.Fatalf("неверная проекция: %+v, %v", states, err)
	}
	if appliedVersion <= before || cache.invalidations != 1 {
		t.Fatalf("batch должен дать новую версию и одну инвалидацию: before=%d after=%d calls=%d", before, appliedVersion, cache.invalidations)
	}

	// Повтор после потери ack: ни состояние, ни версия не меняются, инвалидация повторяется.
	if err := svc.HandleSeatStateBatch(ctx, batch); err != nil {
		t.Fatal(err)
	}
	_, replayVersion, _ := svc.repo.SectorStates(ctx, eventID, sectorID)
	if replayVersion != appliedVersion || cache.invalidations != 2 {
		t.Fatalf("повтор неидемпотентен: version=%d/%d calls=%d", replayVersion, appliedVersion, cache.invalidations)
	}
	// Запоздалый held после free не должен воскресить место.
	if err := svc.HandleSeatStateBatch(ctx, []outbox.Record{seatRecord(t, 1, bookingapi.SeatStateHeld, 1)}); err != nil {
		t.Fatal(err)
	}
	states, lateVersion, _ := svc.repo.SectorStates(ctx, eventID, sectorID)
	if len(states) != 1 || lateVersion != appliedVersion {
		t.Fatalf("старое событие изменило проекцию: %+v version=%d", states, lateVersion)
	}
}

func TestSeatStateBatch_CacheErrorAllowsRetry(t *testing.T) {
	if testing.Short() {
		t.Skip("интеграционный тест: нужен Docker")
	}
	// Основная атомарность проверена выше; здесь fake repo изолирует повтор после ошибки Valkey.
	repo := &fakeRepo{}
	cache := &projectorCache{err: errors.New("valkey down")}
	svc := New(repo, cache, Options{CacheInvalidate: true}, nil, slog.Default())
	record := seatRecord(t, 1, bookingapi.SeatStateHeld, 1)
	if err := svc.HandleSeatStateBatch(context.Background(), []outbox.Record{record}); err == nil {
		t.Fatal("ожидалась ошибка инвалидации")
	}
	cache.err = nil
	if err := svc.HandleSeatStateBatch(context.Background(), []outbox.Record{record}); err != nil {
		t.Fatalf("повтор после восстановления Valkey: %v", err)
	}
}

func (f *fakeRepo) ApplySeatStates(context.Context, []domain.SeatStateChange) ([]domain.SectorVersion, error) {
	return []domain.SectorVersion{{EventID: f.event.ID, SectorID: f.sector.ID, Version: 1}}, nil
}

func seatRecord(t *testing.T, seq int64, state bookingapi.SeatState, seatID int64) outbox.Record {
	t.Helper()
	payload, err := json.Marshal(bookingapi.SeatStateChanged{
		ID: uuid.New(), EventID: uuid.MustParse("10000000-0000-4000-8000-000000000001"), SeatID: seatID, State: state,
	})
	if err != nil {
		t.Fatal(err)
	}
	return outbox.Record{ID: uuid.New(), Seq: seq, Payload: payload}
}

type projectorCache struct {
	Cache
	invalidations int
	err           error
}

func (c *projectorCache) Invalidate(context.Context, uuid.UUID, uuid.UUID, int64) error {
	c.invalidations++
	return c.err
}
