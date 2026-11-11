package valkey

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/nikolaysavelev/soldout/internal/platform/cache"
	"github.com/nikolaysavelev/soldout/internal/queue/app"
	"github.com/nikolaysavelev/soldout/internal/queue/domain"
)

func TestCache(t *testing.T) {
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
	admissions := New(client)

	t.Run("roundtrip", func(t *testing.T) {
		want := domain.Admission{
			Token: uuid.NewString(), EventID: uuid.New(), UserID: uuid.New(),
			IssuedAt:  time.Date(2026, time.November, 10, 18, 0, 0, 0, time.UTC),
			ExpiresAt: time.Date(2026, time.November, 10, 18, 10, 0, 0, time.UTC),
		}
		if err := admissions.Set(ctx, want, time.Minute); err != nil {
			t.Fatalf("Set: %v", err)
		}
		got, err := admissions.Get(ctx, want.Token)
		if err != nil {
			t.Fatalf("Get: %v", err)
		}
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("получен %+v, ожидался %+v", got, want)
		}
	})

	t.Run("miss", func(t *testing.T) {
		_, err := admissions.Get(ctx, uuid.NewString())
		if !errors.Is(err, app.ErrNotFound) {
			t.Fatalf("ожидалась ErrNotFound, получено %v", err)
		}
	})

	t.Run("TTL", func(t *testing.T) {
		a := domain.NewAdmission(uuid.New(), uuid.New(), time.Now(), 100*time.Millisecond)
		if err := admissions.Set(ctx, a, 100*time.Millisecond); err != nil {
			t.Fatalf("Set: %v", err)
		}
		time.Sleep(200 * time.Millisecond)
		_, err := admissions.Get(ctx, a.Token)
		if !errors.Is(err, app.ErrNotFound) {
			t.Fatalf("после TTL ожидалась ErrNotFound, получено %v", err)
		}
	})
}
