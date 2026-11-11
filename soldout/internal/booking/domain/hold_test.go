package domain

import (
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/nikolaysavelev/soldout/internal/booking/api"
)

func newTestHold(seatID int64, now time.Time, ttl time.Duration) Hold {
	return NewHold(uuid.New(), uuid.New(), seatID, uuid.New(), PriceReservation{Sequence: seatID, PriceMinor: 500000}, now, ttl)
}

func TestHold_Expiry(t *testing.T) {
	now := time.Date(2026, 9, 5, 12, 0, 0, 0, time.UTC)
	h := newTestHold(1, now, 10*time.Minute)
	if h.Status != api.HoldActive {
		t.Fatalf("новый hold должен быть active, получено %s", h.Status)
	}
	if h.PriceSequence != 1 || h.PriceMinor != 500000 {
		t.Fatalf("номер и цена должны быть зафиксированы: %+v", h)
	}
	if h.IsExpired(now.Add(9 * time.Minute)) {
		t.Fatal("hold не должен истекать раньше TTL")
	}
	if !h.IsExpired(now.Add(10 * time.Minute)) {
		t.Fatal("hold должен истечь ровно через TTL")
	}
	if !h.IsUsable(now) || h.IsUsable(now.Add(11*time.Minute)) {
		t.Fatal("IsUsable должен учитывать истечение")
	}
}

func TestHold_Transitions(t *testing.T) {
	now := time.Now()
	h := newTestHold(1, now, time.Minute)

	if err := h.Confirm(now.Add(2 * time.Minute)); !errors.Is(err, api.ErrHoldExpired) {
		t.Fatalf("истёкший hold нельзя подтвердить, получено %v", err)
	}
	if err := h.Confirm(now); err != nil || h.Status != api.HoldConfirmed {
		t.Fatalf("active → confirmed: err=%v status=%s", err, h.Status)
	}
	if err := h.Release(); !errors.Is(err, api.ErrHoldNotActive) {
		t.Fatalf("confirmed → released запрещён, получено %v", err)
	}

	h2 := newTestHold(2, now, time.Minute)
	if err := h2.Release(); err != nil || h2.Status != api.HoldReleased {
		t.Fatalf("active → released: err=%v status=%s", err, h2.Status)
	}
	if err := h2.Confirm(now); !errors.Is(err, api.ErrHoldNotActive) {
		t.Fatalf("released → confirmed запрещён, получено %v", err)
	}
}

func TestCanHoldMore_Limit4(t *testing.T) {
	for n, want := range map[int]bool{0: true, 3: true, 4: false, 5: false} {
		if got := CanHoldMore(n); got != want {
			t.Errorf("CanHoldMore(%d) = %v, ожидалось %v", n, got, want)
		}
	}
}
