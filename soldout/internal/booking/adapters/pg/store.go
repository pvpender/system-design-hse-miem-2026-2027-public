// Package pg — PostgreSQL-хранилище booking: holds, orders, order_items.
package pg

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/nikolaysavelev/soldout/internal/booking/api"
	"github.com/nikolaysavelev/soldout/internal/booking/app"
	"github.com/nikolaysavelev/soldout/internal/booking/domain"
	"github.com/nikolaysavelev/soldout/internal/platform/db"
	"github.com/nikolaysavelev/soldout/internal/platform/outbox"
)

// Store — реализация app.Store поверх пула.
type Store struct {
	pool       *db.Pool
	seatOutbox *outbox.Writer
	queries
}

// Option — настройка хранилища.
type Option func(*Store)

// WithOutbox задаёт писателя outbox (режим CDC или polling). По умолчанию — CDC: строка удаляется в той же транзакции.
func WithOutbox(w *outbox.Writer) Option { return func(s *Store) { s.outbox = w } }

// New создаёт хранилище.
func New(pool *db.Pool, opts ...Option) *Store {
	seatOutbox := outbox.NewWriter(outbox.Options{DeleteAfterInsert: false})
	s := &Store{pool: pool, seatOutbox: seatOutbox, queries: queries{
		db: pool, outbox: outbox.NewWriter(outbox.Options{DeleteAfterInsert: true}), seatOutbox: seatOutbox,
	}}
	for _, o := range opts {
		o(s)
	}
	return s
}

// InTx выполняет fn в транзакции.
func (s *Store) InTx(ctx context.Context, fn func(tx app.Tx) error) error {
	return db.WithTx(ctx, s.pool, func(tx pgx.Tx) error {
		return fn(&queries{db: tx, outbox: s.outbox, seatOutbox: s.seatOutbox})
	})
}

// queries — запросы, одинаковые для пула и транзакции.
type queries struct {
	db         db.DBTX
	outbox     *outbox.Writer
	seatOutbox *outbox.Writer
}

// AppendOutbox — событие в outbox в текущей транзакции (platform/outbox).
func (q *queries) AppendOutbox(ctx context.Context, e api.OrderPaidEvent) error {
	return q.outbox.Append(ctx, q.db, e)
}

// AppendSeatStateOutbox — событие карты зала остаётся в outbox до обработки projector'ом.
func (q *queries) AppendSeatStateOutbox(ctx context.Context, e api.SeatStateChanged) error {
	return q.seatOutbox.Append(ctx, q.db, e)
}

var _ app.Tx = (*queries)(nil)

// LockSeat — advisory-lock на (event, seat) до конца транзакции. Ключ: hashtext(event_id), seat_id.
func (q *queries) LockSeat(ctx context.Context, eventID uuid.UUID, seatID int64) error {
	_, err := q.db.Exec(ctx, "/* booking.lock_seat */ SELECT pg_advisory_xact_lock(hashtext($1::text), $2::int)", eventID, seatID)
	if err != nil {
		return fmt.Errorf("booking/pg: lock seat: %w", err)
	}
	return nil
}

// CountUserHolds — активные и подтверждённые hold пользователя на мероприятие (лимит FR-7).
func (q *queries) CountUserHolds(ctx context.Context, eventID, userID uuid.UUID) (int, error) {
	var n int
	err := q.db.QueryRow(ctx, `/* booking.count_user_holds */
		SELECT count(*) FROM holds WHERE event_id = $1 AND user_id = $2 AND status IN ('active', 'confirmed')`,
		eventID, userID).Scan(&n)
	if err != nil {
		return 0, fmt.Errorf("booking/pg: count user holds: %w", err)
	}
	return n, nil
}

// SeatSold — есть ли подтверждённый hold на место (с шага A — по индексу holds_taken_uidx).
func (q *queries) SeatSold(ctx context.Context, eventID uuid.UUID, seatID int64) (bool, error) {
	var sold bool
	err := q.db.QueryRow(ctx, `/* booking.seat_sold */
		SELECT EXISTS (SELECT 1 FROM holds WHERE event_id = $1 AND seat_id = $2 AND status = 'confirmed')`,
		eventID, seatID).Scan(&sold)
	if err != nil {
		return false, fmt.Errorf("booking/pg: seat sold: %w", err)
	}
	return sold, nil
}

// InsertHold — новый hold; нарушение uniq_active_hold / holds_taken_uidx → ErrSeatHeld.
func (q *queries) InsertHold(ctx context.Context, h domain.Hold) error {
	_, err := q.db.Exec(ctx, `/* booking.insert_hold */
		INSERT INTO holds (id, event_id, sector_id, seat_id, user_id, status, price_sequence, price_minor, expires_at, created_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)`,
		h.ID, h.EventID, h.SectorID, h.SeatID, h.UserID, h.Status, h.PriceSequence, h.PriceMinor, h.ExpiresAt, h.CreatedAt)
	if db.IsUniqueViolation(err, "holds_sector_price_sequence_uidx") {
		return fmt.Errorf("booking/pg: номер цены уже сохранён, счётчик Valkey рассинхронизирован: %w", err)
	}
	if db.IsUniqueViolation(err, "") {
		return api.ErrSeatHeld
	}
	if err != nil {
		return fmt.Errorf("booking/pg: insert hold: %w", err)
	}
	return nil
}

const holdCols = "id, event_id, sector_id, seat_id, user_id, status, price_sequence, price_minor, expires_at, created_at"

func scanHold(row pgx.Row) (domain.Hold, error) {
	var h domain.Hold
	err := row.Scan(&h.ID, &h.EventID, &h.SectorID, &h.SeatID, &h.UserID, &h.Status, &h.PriceSequence, &h.PriceMinor, &h.ExpiresAt, &h.CreatedAt)
	return h, err
}

// GetHoldsForUpdate — hold'ы по id с блокировкой строк (в порядке id, чтобы избежать deadlock).
func (q *queries) GetHoldsForUpdate(ctx context.Context, ids []uuid.UUID) ([]domain.Hold, error) {
	rows, err := q.db.Query(ctx, "/* booking.holds_for_update */ SELECT "+holdCols+" FROM holds WHERE id = ANY($1) ORDER BY id FOR UPDATE", ids)
	if err != nil {
		return nil, fmt.Errorf("booking/pg: holds for update: %w", err)
	}
	defer rows.Close()
	var out []domain.Hold
	for rows.Next() {
		h, err := scanHold(rows)
		if err != nil {
			return nil, fmt.Errorf("booking/pg: scan hold: %w", err)
		}
		out = append(out, h)
	}
	return out, rows.Err()
}

// UpdateHold — смена статуса.
func (q *queries) UpdateHold(ctx context.Context, h domain.Hold) error {
	tag, err := q.db.Exec(ctx, "/* booking.update_hold */ UPDATE holds SET status = $2, expires_at = $3 WHERE id = $1", h.ID, h.Status, h.ExpiresAt)
	if err != nil {
		return fmt.Errorf("booking/pg: update hold: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return api.ErrHoldNotFound
	}
	return nil
}

// HoldsInOpenOrders — входит ли хоть один hold в pending/paid заказ.
func (q *queries) HoldsInOpenOrders(ctx context.Context, ids []uuid.UUID) (bool, error) {
	var busy bool
	err := q.db.QueryRow(ctx, `/* booking.holds_in_open_orders */
		SELECT EXISTS (
			SELECT 1 FROM order_items oi JOIN orders o ON o.id = oi.order_id
			WHERE oi.hold_id = ANY($1) AND o.status IN ('pending', 'paid'))`, ids).Scan(&busy)
	if err != nil {
		return false, fmt.Errorf("booking/pg: holds in open orders: %w", err)
	}
	return busy, nil
}

// InsertOrder — заказ и позиции; дубль idempotency_key → ErrIdempotencyConflict.
func (q *queries) InsertOrder(ctx context.Context, o domain.Order) error {
	_, err := q.db.Exec(ctx, `/* booking.insert_order */
		INSERT INTO orders (id, event_id, user_id, status, amount, idempotency_key, created_at, updated_at)
		VALUES ($1, $2, $3, $4, $5::numeric, $6, $7, $8)`,
		o.ID, o.EventID, o.UserID, o.Status, o.AmountString(), o.IdempotencyKey, o.CreatedAt, o.UpdatedAt)
	if db.IsUniqueViolation(err, "orders_idempotency_key_key") {
		return api.ErrIdempotencyConflict
	}
	if err != nil {
		return fmt.Errorf("booking/pg: insert order: %w", err)
	}
	for _, hid := range o.HoldIDs {
		if _, err := q.db.Exec(ctx, "/* booking.insert_order_item */ INSERT INTO order_items (order_id, hold_id) VALUES ($1, $2)", o.ID, hid); err != nil {
			return fmt.Errorf("booking/pg: insert order item: %w", err)
		}
	}
	return nil
}

const orderSelect = `SELECT o.id, o.event_id, o.user_id, o.status, o.amount::text, o.idempotency_key, o.created_at, o.updated_at,
		coalesce(array_agg(oi.hold_id ORDER BY h.id) FILTER (WHERE oi.hold_id IS NOT NULL), '{}'),
		coalesce(array_agg(h.seat_id ORDER BY h.id) FILTER (WHERE h.seat_id IS NOT NULL), '{}')
	FROM orders o
	LEFT JOIN order_items oi ON oi.order_id = o.id
	LEFT JOIN holds h ON h.id = oi.hold_id`

func scanOrder(row pgx.Row) (domain.Order, error) {
	var o domain.Order
	var amount string
	err := row.Scan(&o.ID, &o.EventID, &o.UserID, &o.Status, &amount, &o.IdempotencyKey, &o.CreatedAt, &o.UpdatedAt, &o.HoldIDs, &o.SeatIDs)
	if err != nil {
		return o, err
	}
	o.AmountMinor, err = parseMinor(amount)
	return o, err
}

func parseMinor(amount string) (int64, error) {
	whole, frac, _ := strings.Cut(amount, ".")
	frac = (frac + "00")[:2]
	n, err := strconv.ParseInt(whole+frac, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("booking/pg: amount %q: %w", amount, err)
	}
	return n, nil
}

func (q *queries) getOrder(ctx context.Context, name, where, lock string, arg any) (domain.Order, error) {
	sql := fmt.Sprintf("/* %s */ %s WHERE %s GROUP BY o.id %s", name, orderSelect, where, lock)
	o, err := scanOrder(q.db.QueryRow(ctx, sql, arg))
	if errors.Is(err, pgx.ErrNoRows) {
		return o, api.ErrOrderNotFound
	}
	if err != nil {
		return o, fmt.Errorf("booking/pg: get order: %w", err)
	}
	return o, nil
}

// GetOrderForUpdate — заказ с блокировкой строки (FOR UPDATE OF o).
func (q *queries) GetOrderForUpdate(ctx context.Context, id uuid.UUID) (domain.Order, error) {
	// FOR UPDATE несовместим с GROUP BY, поэтому блокируем отдельным запросом.
	if _, err := q.db.Exec(ctx, "/* booking.lock_order */ SELECT id FROM orders WHERE id = $1 FOR UPDATE", id); err != nil {
		return domain.Order{}, fmt.Errorf("booking/pg: lock order: %w", err)
	}
	return q.getOrder(ctx, "booking.get_order_locked", "o.id = $1", "", id)
}

// UpdateOrder — смена статуса.
func (q *queries) UpdateOrder(ctx context.Context, o domain.Order) error {
	tag, err := q.db.Exec(ctx, "/* booking.update_order */ UPDATE orders SET status = $2, updated_at = $3 WHERE id = $1", o.ID, o.Status, o.UpdatedAt)
	if err != nil {
		return fmt.Errorf("booking/pg: update order: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return api.ErrOrderNotFound
	}
	return nil
}

// GetHold — чтение без блокировки.
func (q *queries) GetHold(ctx context.Context, id uuid.UUID) (domain.Hold, error) {
	h, err := scanHold(q.db.QueryRow(ctx, "/* booking.get_hold */ SELECT "+holdCols+" FROM holds WHERE id = $1", id))
	if errors.Is(err, pgx.ErrNoRows) {
		return h, api.ErrHoldNotFound
	}
	if err != nil {
		return h, fmt.Errorf("booking/pg: get hold: %w", err)
	}
	return h, nil
}

// GetOrder — чтение без блокировки.
func (q *queries) GetOrder(ctx context.Context, id uuid.UUID) (domain.Order, error) {
	return q.getOrder(ctx, "booking.get_order", "o.id = $1", "", id)
}

// FindOrderByIdempotencyKey — заказ по ключу идемпотентности.
func (q *queries) FindOrderByIdempotencyKey(ctx context.Context, key string) (domain.Order, error) {
	return q.getOrder(ctx, "booking.get_order_by_key", "o.idempotency_key = $1", "", key)
}

// ListUserOrders — заказы пользователя (новые первыми).
func (q *queries) ListUserOrders(ctx context.Context, userID uuid.UUID) ([]domain.Order, error) {
	rows, err := q.db.Query(ctx, "/* booking.list_user_orders */ "+orderSelect+" WHERE o.user_id = $1 GROUP BY o.id ORDER BY o.created_at DESC", userID)
	if err != nil {
		return nil, fmt.Errorf("booking/pg: list user orders: %w", err)
	}
	defer rows.Close()
	var out []domain.Order
	for rows.Next() {
		o, err := scanOrder(rows)
		if err != nil {
			return nil, fmt.Errorf("booking/pg: scan order: %w", err)
		}
		out = append(out, o)
	}
	return out, rows.Err()
}

// SeatStates — held/sold по мероприятию. Индекса по (event_id, status) нет намеренно (ADR-001).
func (q *queries) SeatStates(ctx context.Context, eventID uuid.UUID) (map[int64]api.SeatState, error) {
	rows, err := q.db.Query(ctx, `/* booking.seat_states */
		SELECT seat_id, status FROM holds WHERE event_id = $1 AND status IN ('active', 'confirmed')`, eventID)
	if err != nil {
		return nil, fmt.Errorf("booking/pg: seat states: %w", err)
	}
	defer rows.Close()
	out := map[int64]api.SeatState{}
	for rows.Next() {
		var seatID int64
		var status api.HoldStatus
		if err := rows.Scan(&seatID, &status); err != nil {
			return nil, fmt.Errorf("booking/pg: scan seat state: %w", err)
		}
		if status == api.HoldConfirmed {
			out[seatID] = api.SeatStateSold
		} else if _, sold := out[seatID]; !sold {
			out[seatID] = api.SeatStateHeld
		}
	}
	return out, rows.Err()
}

// SoldCount — подтверждённые hold мероприятия.
func (q *queries) SoldCount(ctx context.Context, eventID uuid.UUID) (int, error) {
	var n int
	if err := q.db.QueryRow(ctx, "/* booking.sold_count */ SELECT count(*) FROM holds WHERE event_id = $1 AND status = 'confirmed'", eventID).Scan(&n); err != nil {
		return 0, fmt.Errorf("booking/pg: sold count: %w", err)
	}
	return n, nil
}

// ExpireHoldsBatch — до limit просроченных active hold → released одним UPDATE; конкурентные
// транзакции (pay, release) не блокируются: строки берутся FOR UPDATE SKIP LOCKED.
func (s *Store) ExpireHoldsBatch(ctx context.Context, now time.Time, limit int) (out []app.ReleasedHold, err error) {
	err = db.WithTx(ctx, s.pool, func(tx pgx.Tx) error {
		rows, err := tx.Query(ctx, `/* booking.expire_holds_batch */
			WITH batch AS (
				SELECT id FROM holds WHERE status = 'active' AND expires_at <= $1
				ORDER BY expires_at LIMIT $2 FOR UPDATE SKIP LOCKED)
			UPDATE holds h SET status = 'released' FROM batch WHERE h.id = batch.id
			RETURNING h.id, h.event_id, h.seat_id`, now, limit)
		if err != nil {
			return fmt.Errorf("booking/pg: expire holds: %w", err)
		}
		defer rows.Close()
		out = out[:0]
		for rows.Next() {
			var r app.ReleasedHold
			if err := rows.Scan(&r.ID, &r.EventID, &r.SeatID); err != nil {
				return fmt.Errorf("booking/pg: scan released: %w", err)
			}
			out = append(out, r)
		}
		if err := rows.Err(); err != nil {
			return err
		}
		events := make([]outbox.Event, 0, len(out))
		for _, h := range out {
			events = append(events, api.SeatStateChanged{
				ID: uuid.New(), EventID: h.EventID, SeatID: h.SeatID, State: api.SeatStateFree, HoldID: h.ID, At: now,
			})
		}
		return s.seatOutbox.AppendBatch(ctx, tx, events)
	})
	return out, err
}

// ExpireOrders — pending заказы, у которых хотя бы один hold released → expired.
func (q *queries) ExpireOrders(ctx context.Context, now time.Time) (int64, error) {
	tag, err := q.db.Exec(ctx, `/* booking.expire_orders */
		UPDATE orders SET status = 'expired', updated_at = $1
		WHERE status = 'pending' AND EXISTS (
			SELECT 1 FROM order_items oi JOIN holds h ON h.id = oi.hold_id
			WHERE oi.order_id = orders.id AND h.status = 'released')`, now)
	if err != nil {
		return 0, fmt.Errorf("booking/pg: expire orders: %w", err)
	}
	return tag.RowsAffected(), nil
}

// CountActiveHolds — для gauge holds_active.
func (q *queries) CountActiveHolds(ctx context.Context) (int64, error) {
	var n int64
	if err := q.db.QueryRow(ctx, "/* booking.count_active_holds */ SELECT count(*) FROM holds WHERE status = 'active'").Scan(&n); err != nil {
		return 0, fmt.Errorf("booking/pg: count active holds: %w", err)
	}
	return n, nil
}
