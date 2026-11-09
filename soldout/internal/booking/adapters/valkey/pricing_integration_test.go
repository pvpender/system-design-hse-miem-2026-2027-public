package valkey

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"

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

	other, err := pricer.ReservePrice(ctx, uuid.New(), 500000, 10, 10)
	if err != nil || other.Sequence != 1 || other.PriceMinor != 500000 {
		t.Fatalf("счётчик другого сектора должен начинаться с 1: %+v, %v", other, err)
	}
}
