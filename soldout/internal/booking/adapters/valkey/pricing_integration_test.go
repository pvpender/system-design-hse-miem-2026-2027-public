package valkey

import (
	"context"
	"errors"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/nikolaysavelev/soldout/internal/booking/api"
	"github.com/nikolaysavelev/soldout/internal/booking/domain"
	"github.com/nikolaysavelev/soldout/internal/platform/cache"
)

func TestReservePrice_ConcurrentAtomicSequence(t *testing.T) {
	if testing.Short() {
		t.Skip("интеграционный тест: нужен Docker (пропуск с -short)")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{ContainerRequest: testcontainers.ContainerRequest{
		Image: "valkey/valkey:9-alpine", ExposedPorts: []string{"6379/tcp"}, WaitingFor: wait.ForListeningPort("6379/tcp"),
	}, Started: true})
	if err != nil {
		t.Fatalf("valkey container: %v", err)
	}
	t.Cleanup(func() { _ = container.Terminate(context.Background()) })
	addr, err := container.Endpoint(ctx, "")
	if err != nil {
		t.Fatal(err)
	}
	client, err := cache.New(ctx, addr)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(client.Close)
	pricer := New(client)
	sectorID := uuid.New()

	const workers = 100
	results := make(chan domain.PriceReservation, workers)
	errs := make(chan error, workers)
	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			reservation, err := pricer.ReservePrice(ctx, sectorID, 500000, 10, 10)
			if err != nil {
				errs <- err
				return
			}
			results <- reservation
		}()
	}
	wg.Wait()
	close(results)
	close(errs)
	for err := range errs {
		t.Errorf("ReservePrice: %v", err)
	}

	seen := make(map[int64]int64, workers)
	for reservation := range results {
		want, err := domain.PriceForSequence(500000, 10, 10, reservation.Sequence)
		if err != nil || reservation.PriceMinor != want {
			t.Errorf("номер %d: цена %d, %v; ожидалось %d", reservation.Sequence, reservation.PriceMinor, err, want)
		}
		seen[reservation.Sequence]++
	}
	if len(seen) != workers {
		t.Fatalf("ожидалось %d уникальных номеров, получено %d", workers, len(seen))
	}
	for sequence := int64(1); sequence <= workers; sequence++ {
		if seen[sequence] != 1 {
			t.Errorf("номер %d выделен %d раз", sequence, seen[sequence])
		}
	}

	// При переполнении int64 Lua не должна отдавать MinInt64 в PostgreSQL.
	for _, overflowSequence := range []int64{32301, 40259} {
		if _, err := domain.PriceForSequence(500000, 10, 100, overflowSequence); err == nil {
			t.Fatalf("тестовый номер %d должен вызывать переполнение цены", overflowSequence)
		}
		if err := client.Do(ctx, client.B().Set().Key(key(sectorID)).Value(strconv.FormatInt(overflowSequence-1, 10)).Build()).Error(); err != nil {
			t.Fatalf("установить счётчик перед переполнением: %v", err)
		}
		reservation, err := pricer.ReservePrice(ctx, sectorID, 500000, 10, 100)
		if err == nil {
			t.Fatalf("переполнение цены на номере %d должно возвращать ошибку, получено %+v", overflowSequence, reservation)
		}
		if !errors.Is(err, api.ErrPriceOverflow) {
			t.Fatalf("переполнение цены на номере %d должно быть ErrPriceOverflow: %v", overflowSequence, err)
		}
	}

	other, err := pricer.ReservePrice(ctx, uuid.New(), 500000, 10, 10)
	if err != nil || other.Sequence != 1 || other.PriceMinor != 500000 {
		t.Fatalf("счётчик другого сектора должен начинаться с 1: %+v, %v", other, err)
	}
}
