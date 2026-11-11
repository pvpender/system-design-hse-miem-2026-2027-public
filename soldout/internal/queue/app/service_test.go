package app

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"

	catalogapi "github.com/nikolaysavelev/soldout/internal/catalog/api"
	"github.com/nikolaysavelev/soldout/internal/queue/api"
	"github.com/nikolaysavelev/soldout/internal/queue/domain"
)

type fakeCache struct {
	admission domain.Admission
	setErr    error
	getErr    error
	setCalls  int
	getCalls  int
	setTTL    time.Duration
}

func (f *fakeCache) Set(_ context.Context, a domain.Admission, ttl time.Duration) error {
	f.setCalls++
	f.setTTL = ttl
	if f.setErr != nil {
		return f.setErr
	}
	f.admission = a
	return nil
}

func (f *fakeCache) Get(_ context.Context, _ string) (domain.Admission, error) {
	f.getCalls++
	if f.getErr != nil {
		return domain.Admission{}, f.getErr
	}
	return f.admission, nil
}

type fakeCatalog struct {
	getEventErr error
}

func (f fakeCatalog) ListEvents(context.Context) ([]catalogapi.Event, error) { return nil, nil }
func (f fakeCatalog) GetEvent(_ context.Context, id uuid.UUID) (catalogapi.Event, error) {
	if f.getEventErr != nil {
		return catalogapi.Event{}, f.getEventErr
	}
	return catalogapi.Event{ID: id}, nil
}
func (f fakeCatalog) GetSeat(context.Context, int64) (catalogapi.Seat, error) {
	return catalogapi.Seat{}, nil
}
func (f fakeCatalog) SeatMap(context.Context, uuid.UUID) (catalogapi.SeatMap, error) {
	return catalogapi.SeatMap{}, nil
}
func (f fakeCatalog) SectorSeatMap(context.Context, uuid.UUID, uuid.UUID) (catalogapi.SectorSeatMapResult, error) {
	return catalogapi.SectorSeatMapResult{}, nil
}
func (f fakeCatalog) CreateEvent(context.Context, catalogapi.CreateEventInput) (catalogapi.Event, error) {
	return catalogapi.Event{}, nil
}
func (f fakeCatalog) OpenSales(context.Context, uuid.UUID) (catalogapi.Event, error) {
	return catalogapi.Event{}, nil
}

func TestServiceJoin(t *testing.T) {
	ctx := context.Background()
	eventID, userID := uuid.New(), uuid.New()
	now := time.Date(2026, time.November, 10, 18, 0, 0, 0, time.UTC)
	ttl := 10 * time.Minute

	t.Run("success", func(t *testing.T) {
		cache := &fakeCache{}
		svc := New(cache, fakeCatalog{}, ttl)
		svc.now = func() time.Time { return now }

		got, err := svc.Join(ctx, eventID, userID)
		if err != nil {
			t.Fatalf("Join: %v", err)
		}
		if got.Token == "" || got.EventID != eventID || got.UserID != userID {
			t.Fatalf("неожиданный допуск: %+v", got)
		}
		if !got.IssuedAt.Equal(now) || !got.ExpiresAt.Equal(now.Add(ttl)) {
			t.Fatalf("неожиданные сроки допуска: %+v", got)
		}
		if cache.setCalls != 1 || cache.setTTL != ttl {
			t.Fatalf("Set вызван %d раз с TTL %v", cache.setCalls, cache.setTTL)
		}
		if cache.admission.Token != got.Token || cache.admission.EventID != eventID || cache.admission.UserID != userID {
			t.Fatalf("в Valkey сохранён другой допуск: %+v", cache.admission)
		}
	})

	t.Run("unknown event", func(t *testing.T) {
		cache := &fakeCache{}
		svc := New(cache, fakeCatalog{getEventErr: catalogapi.ErrEventNotFound}, ttl)
		_, err := svc.Join(ctx, eventID, userID)
		if !errors.Is(err, catalogapi.ErrEventNotFound) {
			t.Fatalf("ожидалась ErrEventNotFound, получено %v", err)
		}
		if cache.setCalls != 0 {
			t.Fatal("для неизвестного мероприятия Set не должен вызываться")
		}
	})

	t.Run("nil user", func(t *testing.T) {
		cache := &fakeCache{}
		svc := New(cache, fakeCatalog{}, ttl)
		_, err := svc.Join(ctx, eventID, uuid.Nil)
		if !errors.Is(err, api.ErrAdmissionInvalid) {
			t.Fatalf("ожидалась ErrAdmissionInvalid, получено %v", err)
		}
		if cache.setCalls != 0 {
			t.Fatal("для пустого user_id Set не должен вызываться")
		}
	})

	t.Run("set error", func(t *testing.T) {
		setErr := errors.New("valkey unavailable")
		cache := &fakeCache{setErr: setErr}
		svc := New(cache, fakeCatalog{}, ttl)
		got, err := svc.Join(ctx, eventID, userID)
		if !errors.Is(err, setErr) {
			t.Fatalf("ожидалась ошибка SET, получено %v", err)
		}
		if got != (api.Admission{}) {
			t.Fatalf("при ошибке SET допуск не должен возвращаться: %+v", got)
		}
	})
}

func TestServiceValidate(t *testing.T) {
	ctx := context.Background()
	eventID, userID := uuid.New(), uuid.New()
	now := time.Date(2026, time.November, 10, 18, 0, 0, 0, time.UTC)
	valid := domain.NewAdmission(eventID, userID, now.Add(-time.Minute), 10*time.Minute)
	getErr := errors.New("valkey unavailable")

	tests := []struct {
		name    string
		cache   *fakeCache
		eventID uuid.UUID
		userID  uuid.UUID
		wantErr error
	}{
		{name: "valid", cache: &fakeCache{admission: valid}, eventID: eventID, userID: userID},
		{name: "miss", cache: &fakeCache{getErr: ErrNotFound}, eventID: eventID, userID: userID, wantErr: api.ErrAdmissionInvalid},
		{name: "invalid user", cache: &fakeCache{admission: valid}, eventID: eventID, userID: uuid.New(), wantErr: api.ErrAdmissionInvalid},
		{name: "invalid event", cache: &fakeCache{admission: valid}, eventID: uuid.New(), userID: userID, wantErr: api.ErrAdmissionInvalid},
		{name: "expired", cache: &fakeCache{admission: domain.NewAdmission(eventID, userID, now.Add(-10*time.Minute), time.Minute)}, eventID: eventID, userID: userID, wantErr: api.ErrAdmissionInvalid},
		{name: "get error", cache: &fakeCache{getErr: getErr}, eventID: eventID, userID: userID, wantErr: getErr},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			svc := New(tt.cache, fakeCatalog{}, 10*time.Minute)
			svc.now = func() time.Time { return now }
			err := svc.Validate(ctx, valid.Token, tt.eventID, tt.userID)
			if !errors.Is(err, tt.wantErr) {
				t.Fatalf("ожидалась ошибка %v, получено %v", tt.wantErr, err)
			}
			if tt.cache.getCalls != 1 {
				t.Fatalf("Get вызван %d раз, ожидался 1", tt.cache.getCalls)
			}
		})
	}
}
