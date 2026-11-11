package pg_test

import (
	"context"
	"log/slog"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/testcontainers/testcontainers-go"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"

	catalogpg "github.com/nikolaysavelev/soldout/internal/catalog/adapters/pg"
	"github.com/nikolaysavelev/soldout/internal/catalog/api"
	"github.com/nikolaysavelev/soldout/internal/catalog/domain"
	"github.com/nikolaysavelev/soldout/internal/platform/db"
)

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
	if err := db.Seed(ctx, pool, 3000, slog.Default()); err != nil {
		t.Fatalf("seed: %v", err)
	}
	return pool
}

// Две реплики монолита публикуют события одного места в свои шины: порядок применения любой.
// Итог проекции обязан совпасть с истиной при каждом порядке.
func TestApplySeatState_OrderIndependent(t *testing.T) {
	pool := startDB(t)
	repo := catalogpg.New(pool)
	ctx := context.Background()
	eventID := uuid.MustParse("10000000-0000-4000-8000-000000000001")
	h1, h2 := uuid.New(), uuid.New()

	type step struct {
		state api.SeatStatus
		hold  uuid.UUID
	}
	cases := []struct {
		name  string
		seat  int64
		steps []step
		want  string // "" — строки нет (место свободно)
	}{
		{"held затем sold", 1, []step{{api.SeatHeld, h1}, {api.SeatSold, h1}}, "sold"},
		{"sold раньше held того же hold", 2, []step{{api.SeatSold, h1}, {api.SeatHeld, h1}}, "sold"},
		{"held затем free", 3, []step{{api.SeatHeld, h1}, {api.SeatFree, h1}}, ""},
		{"free старого hold после held нового", 4, []step{{api.SeatHeld, h1}, {api.SeatHeld, h2}, {api.SeatFree, h1}}, "held"},
		{"free не снимает sold", 5, []step{{api.SeatHeld, h1}, {api.SeatSold, h1}, {api.SeatFree, h1}}, "sold"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			for i, s := range c.steps {
				_, err := repo.ApplySeatStates(ctx, []domain.SeatStateChange{{
					ID: uuid.New(), SourceSeq: int64(i + 1), EventID: eventID, SeatID: c.seat, State: s.state, HoldID: s.hold,
				}})
				if err != nil {
					t.Fatal(err)
				}
			}
			var got string
			err := pool.QueryRow(ctx, `SELECT coalesce((SELECT state FROM catalog_seat_state WHERE event_id = $1 AND seat_id = $2), '')`, eventID, c.seat).Scan(&got)
			if err != nil {
				t.Fatal(err)
			}
			if got != c.want {
				t.Fatalf("проекция = %q, ожидалось %q", got, c.want)
			}
		})
	}
}
