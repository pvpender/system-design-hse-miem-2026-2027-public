package domain

import (
	"fmt"
	"time"

	"github.com/google/uuid"

	"github.com/nikolaysavelev/soldout/internal/booking/api"
)

// Order — заказ на один или несколько hold одного пользователя и мероприятия.
type Order struct {
	ID             uuid.UUID
	EventID        uuid.UUID
	UserID         uuid.UUID
	Status         api.OrderStatus
	AmountMinor    int64
	HoldIDs        []uuid.UUID
	SeatIDs        []int64
	IdempotencyKey string
	CreatedAt      time.Time
	UpdatedAt      time.Time
}

// NewOrder собирает заказ из hold'ов. Правила: 1..4 hold, все usable, один пользователь, одно мероприятие.
// Сумма заказа — сумма цен, зафиксированных при создании hold'ов.
func NewOrder(holds []Hold, userID uuid.UUID, idempotencyKey string, now time.Time) (Order, error) {
	if len(holds) == 0 {
		return Order{}, fmt.Errorf("%w: пустой список hold_ids", api.ErrValidation)
	}
	if len(holds) > api.MaxTicketsPerUser {
		return Order{}, fmt.Errorf("%w: в заказе не более %d мест", api.ErrHoldLimit, api.MaxTicketsPerUser)
	}
	if idempotencyKey == "" {
		return Order{}, fmt.Errorf("%w: пустой Idempotency-Key", api.ErrValidation)
	}
	eventID := holds[0].EventID
	o := Order{
		ID: uuid.New(), EventID: eventID, UserID: userID, Status: api.OrderPending,
		IdempotencyKey: idempotencyKey, CreatedAt: now, UpdatedAt: now,
	}
	seen := map[uuid.UUID]bool{}
	for _, h := range holds {
		if seen[h.ID] {
			return Order{}, fmt.Errorf("%w: hold %s указан дважды", api.ErrValidation, h.ID)
		}
		seen[h.ID] = true
		if h.UserID != userID || h.EventID != eventID {
			return Order{}, api.ErrHoldsMismatch
		}
		if h.Status != api.HoldActive {
			return Order{}, fmt.Errorf("%w: hold %s в статусе %s", api.ErrHoldNotActive, h.ID, h.Status)
		}
		if h.IsExpired(now) {
			return Order{}, fmt.Errorf("%w: hold %s", api.ErrHoldExpired, h.ID)
		}
		o.HoldIDs = append(o.HoldIDs, h.ID)
		o.SeatIDs = append(o.SeatIDs, h.SeatID)
		o.AmountMinor += h.PriceMinor
	}
	return o, nil
}

// MarkPaid — pending → paid.
func (o *Order) MarkPaid(now time.Time) error { return o.transition(api.OrderPaid, now) }

// MarkFailed — pending → failed.
func (o *Order) MarkFailed(now time.Time) error { return o.transition(api.OrderFailed, now) }

// MarkExpired — pending → expired.
func (o *Order) MarkExpired(now time.Time) error { return o.transition(api.OrderExpired, now) }

func (o *Order) transition(to api.OrderStatus, now time.Time) error {
	if o.Status != api.OrderPending {
		return fmt.Errorf("%w: заказ %s в статусе %s", api.ErrOrderNotPending, o.ID, o.Status)
	}
	o.Status = to
	o.UpdatedAt = now
	return nil
}

// AmountString — сумма в рублях с копейками для numeric(12,2).
func (o Order) AmountString() string { return FormatMinor(o.AmountMinor) }

// FormatMinor форматирует копейки как "5000.00".
func FormatMinor(minor int64) string {
	sign := ""
	if minor < 0 {
		sign, minor = "-", -minor
	}
	return fmt.Sprintf("%s%d.%02d", sign, minor/100, minor%100)
}

// ToAPI — DTO.
func (o Order) ToAPI() api.Order {
	holdIDs := append([]uuid.UUID{}, o.HoldIDs...)
	seatIDs := append([]int64{}, o.SeatIDs...)
	return api.Order{
		ID: o.ID, EventID: o.EventID, UserID: o.UserID, Status: o.Status,
		AmountMinor: o.AmountMinor, Amount: o.AmountString(), HoldIDs: holdIDs, SeatIDs: seatIDs,
		IdempotencyKey: o.IdempotencyKey, CreatedAt: o.CreatedAt, UpdatedAt: o.UpdatedAt,
	}
}
