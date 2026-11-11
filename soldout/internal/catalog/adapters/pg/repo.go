// Package pg — PostgreSQL-хранилище каталога: venues, sectors, seats, events и проекция карты зала
// (catalog_seat_state, catalog_sector_version). Таблицы других модулей не читаются.
package pg

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/nikolaysavelev/soldout/internal/catalog/api"
	"github.com/nikolaysavelev/soldout/internal/catalog/domain"
	"github.com/nikolaysavelev/soldout/internal/platform/db"
)

// Repo — реализация app.Repository.
type Repo struct {
	db   db.DBTX
	pool *db.Pool
}

// New создаёт репозиторий поверх пула (пул нужен для транзакции подписчика).
func New(pool *db.Pool) *Repo { return &Repo{db: pool, pool: pool} }

const eventCols = "id, name, venue_id, starts_at, sales_open_at, sales_state"

func scanEvent(row pgx.Row) (domain.Event, error) {
	var e domain.Event
	err := row.Scan(&e.ID, &e.Name, &e.VenueID, &e.StartsAt, &e.SalesOpenAt, &e.SalesState)
	if errors.Is(err, pgx.ErrNoRows) {
		return e, api.ErrEventNotFound
	}
	if err != nil {
		return e, fmt.Errorf("catalog/pg: scan event: %w", err)
	}
	return e, nil
}

// ListEvents — все мероприятия по дате начала.
func (r *Repo) ListEvents(ctx context.Context) ([]domain.Event, error) {
	rows, err := r.db.Query(ctx, "/* catalog.list_events */ SELECT "+eventCols+" FROM events ORDER BY starts_at")
	if err != nil {
		return nil, fmt.Errorf("catalog/pg: list events: %w", err)
	}
	defer rows.Close()
	var out []domain.Event
	for rows.Next() {
		e, err := scanEvent(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, e)
	}
	return out, rows.Err()
}

// GetEvent — мероприятие по id.
func (r *Repo) GetEvent(ctx context.Context, id uuid.UUID) (domain.Event, error) {
	return scanEvent(r.db.QueryRow(ctx, "/* catalog.get_event */ SELECT "+eventCols+" FROM events WHERE id = $1", id))
}

// GetSeat — место с площадкой и настройками цены сектора.
func (r *Repo) GetSeat(ctx context.Context, id int64) (domain.Seat, error) {
	var s domain.Seat
	err := r.db.QueryRow(ctx, `/* catalog.get_seat */
		SELECT s.id, s.sector_id, sec.venue_id, s.row_no, s.seat_no,
		       sec.base_price_minor, sec.price_increase_percent, sec.price_step_size
		FROM seats s JOIN sectors sec ON sec.id = s.sector_id
		WHERE s.id = $1`, id).Scan(&s.ID, &s.SectorID, &s.VenueID, &s.RowNo, &s.SeatNo,
		&s.Pricing.BasePriceMinor, &s.Pricing.IncreasePercent, &s.Pricing.StepSize)
	if errors.Is(err, pgx.ErrNoRows) {
		return s, api.ErrSeatNotFound
	}
	if err != nil {
		return s, fmt.Errorf("catalog/pg: get seat: %w", err)
	}
	return s, nil
}

// SectorsByVenue — секторы площадки.
func (r *Repo) SectorsByVenue(ctx context.Context, venueID uuid.UUID) ([]domain.Sector, error) {
	rows, err := r.db.Query(ctx, `/* catalog.sectors_by_venue */
		SELECT id, venue_id, name, kind, capacity FROM sectors WHERE venue_id = $1 ORDER BY name`, venueID)
	if err != nil {
		return nil, fmt.Errorf("catalog/pg: sectors: %w", err)
	}
	defer rows.Close()
	var out []domain.Sector
	for rows.Next() {
		var s domain.Sector
		if err := rows.Scan(&s.ID, &s.VenueID, &s.Name, &s.Kind, &s.Capacity); err != nil {
			return nil, fmt.Errorf("catalog/pg: scan sector: %w", err)
		}
		out = append(out, s)
	}
	return out, rows.Err()
}

// GetSector — сектор по id.
func (r *Repo) GetSector(ctx context.Context, id uuid.UUID) (domain.Sector, error) {
	var s domain.Sector
	err := r.db.QueryRow(ctx, "/* catalog.get_sector */ SELECT id, venue_id, name, kind, capacity FROM sectors WHERE id = $1", id).
		Scan(&s.ID, &s.VenueID, &s.Name, &s.Kind, &s.Capacity)
	if errors.Is(err, pgx.ErrNoRows) {
		return s, api.ErrSectorNotFound
	}
	if err != nil {
		return s, fmt.Errorf("catalog/pg: get sector: %w", err)
	}
	return s, nil
}

// SeatsBySector — места одного сектора (≈ 1 000 строк вместо 40 000 на всю площадку).
func (r *Repo) SeatsBySector(ctx context.Context, sectorID uuid.UUID) ([]domain.Seat, error) {
	rows, err := r.db.Query(ctx, `/* catalog.seats_by_sector */
		SELECT s.id, s.sector_id, sec.venue_id, s.row_no, s.seat_no
		FROM seats s JOIN sectors sec ON sec.id = s.sector_id
		WHERE s.sector_id = $1
		ORDER BY s.id`, sectorID)
	if err != nil {
		return nil, fmt.Errorf("catalog/pg: seats by sector: %w", err)
	}
	defer rows.Close()
	out := make([]domain.Seat, 0, 1000)
	for rows.Next() {
		var s domain.Seat
		if err := rows.Scan(&s.ID, &s.SectorID, &s.VenueID, &s.RowNo, &s.SeatNo); err != nil {
			return nil, fmt.Errorf("catalog/pg: scan seat: %w", err)
		}
		out = append(out, s)
	}
	return out, rows.Err()
}

// SectorCounts — held/sold и версия по секторам мероприятия из проекции.
func (r *Repo) SectorCounts(ctx context.Context, eventID uuid.UUID) (map[uuid.UUID]domain.SectorCounts, error) {
	out := map[uuid.UUID]domain.SectorCounts{}
	rows, err := r.db.Query(ctx, `/* catalog.sector_counts */
		SELECT sector_id, state, count(*) FROM catalog_seat_state WHERE event_id = $1 GROUP BY sector_id, state`, eventID)
	if err != nil {
		return nil, fmt.Errorf("catalog/pg: sector counts: %w", err)
	}
	for rows.Next() {
		var id uuid.UUID
		var state string
		var n int
		if err := rows.Scan(&id, &state, &n); err != nil {
			rows.Close()
			return nil, fmt.Errorf("catalog/pg: scan counts: %w", err)
		}
		c := out[id]
		if state == string(api.SeatSold) {
			c.Sold = n
		} else {
			c.Held = n
		}
		out[id] = c
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, err
	}
	vrows, err := r.db.Query(ctx, "/* catalog.sector_versions */ SELECT sector_id, version FROM catalog_sector_version WHERE event_id = $1", eventID)
	if err != nil {
		return nil, fmt.Errorf("catalog/pg: sector versions: %w", err)
	}
	defer vrows.Close()
	for vrows.Next() {
		var id uuid.UUID
		var v int64
		if err := vrows.Scan(&id, &v); err != nil {
			return nil, fmt.Errorf("catalog/pg: scan version: %w", err)
		}
		c := out[id]
		c.Version = v
		out[id] = c
	}
	return out, vrows.Err()
}

// SectorStates — статусы мест сектора и версия сектора одним снимком (REPEATABLE READ), чтобы версия
// соответствовала данным, которые уйдут в кэш.
func (r *Repo) SectorStates(ctx context.Context, eventID, sectorID uuid.UUID) ([]domain.SeatState, int64, error) {
	var states []domain.SeatState
	var version int64
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return nil, 0, err
	}
	defer tx.Rollback(ctx) //nolint:errcheck
	if _, err := tx.Exec(ctx, "/* catalog.snapshot */ SET TRANSACTION ISOLATION LEVEL REPEATABLE READ"); err != nil {
		return nil, 0, fmt.Errorf("catalog/pg: snapshot: %w", err)
	}
	err = tx.QueryRow(ctx, "/* catalog.sector_version */ SELECT coalesce((SELECT version FROM catalog_sector_version WHERE event_id = $1 AND sector_id = $2), 0)", eventID, sectorID).Scan(&version)
	if err != nil {
		return nil, 0, fmt.Errorf("catalog/pg: sector version: %w", err)
	}
	rows, err := tx.Query(ctx, "/* catalog.sector_states */ SELECT seat_id, state FROM catalog_seat_state WHERE event_id = $1 AND sector_id = $2", eventID, sectorID)
	if err != nil {
		return nil, 0, fmt.Errorf("catalog/pg: sector states: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var st domain.SeatState
		var state string
		if err := rows.Scan(&st.SeatID, &state); err != nil {
			return nil, 0, fmt.Errorf("catalog/pg: scan state: %w", err)
		}
		st.Status = api.SeatStatus(state)
		states = append(states, st)
	}
	if err := rows.Err(); err != nil {
		return nil, 0, err
	}
	return states, version, tx.Commit(ctx)
}

// ApplySeatStates — одна set-based транзакция на batch. Дедупликация по id и high-watermark по seq
// делают повторную доставку безопасной; free не удаляет high-watermark места.
func (r *Repo) ApplySeatStates(ctx context.Context, changes []domain.SeatStateChange) ([]domain.SectorVersion, error) {
	if len(changes) == 0 {
		return nil, nil
	}
	payload, err := json.Marshal(changes)
	if err != nil {
		return nil, fmt.Errorf("catalog/pg: marshal seat changes: %w", err)
	}
	rows, err := r.pool.Query(ctx, `/* catalog.apply_seat_state_batch */
		WITH input AS (
			SELECT * FROM jsonb_to_recordset($1::jsonb)
			AS x(id uuid, source_seq bigint, event_id uuid, seat_id bigint, state text, hold_id uuid)
		),
		inserted AS (
			INSERT INTO catalog_processed_events(id) SELECT id FROM input ON CONFLICT DO NOTHING RETURNING id
		),
		fresh AS (
			SELECT i.* FROM input i JOIN inserted p USING (id)
		),
		latest AS (
			SELECT DISTINCT ON (event_id, seat_id) * FROM fresh
			ORDER BY event_id, seat_id, source_seq DESC
		),
		resolved AS (
			SELECT l.*, s.sector_id FROM latest l JOIN seats s ON s.id = l.seat_id
		),
		advanced AS (
			INSERT INTO catalog_seat_projection_position(event_id, seat_id, last_source_seq)
			SELECT event_id, seat_id, source_seq FROM resolved
			ON CONFLICT (event_id, seat_id) DO UPDATE SET last_source_seq = EXCLUDED.last_source_seq
			WHERE catalog_seat_projection_position.last_source_seq < EXCLUDED.last_source_seq
			RETURNING event_id, seat_id, last_source_seq
		),
		changed AS (
			SELECT r.* FROM resolved r JOIN advanced a USING (event_id, seat_id)
		),
		sector_changes AS (
			SELECT event_id, sector_id, count(*) AS changes FROM changed GROUP BY event_id, sector_id
		),
		bumped AS (
			INSERT INTO catalog_sector_version(event_id, sector_id, version, updated_at)
			SELECT event_id, sector_id, changes, now() FROM sector_changes
			ON CONFLICT (event_id, sector_id) DO UPDATE
			SET version = catalog_sector_version.version + EXCLUDED.version, updated_at = now()
			RETURNING event_id, sector_id, version
		),
		deleted AS (
			DELETE FROM catalog_seat_state c USING changed ch
			WHERE c.event_id = ch.event_id AND c.seat_id = ch.seat_id AND ch.state = 'free'
				AND c.state = 'held' AND (c.hold_id = ch.hold_id OR c.hold_id IS NULL)
			RETURNING c.event_id, c.seat_id
		),
		upserted AS (
			INSERT INTO catalog_seat_state(event_id, seat_id, sector_id, state, version, hold_id, updated_at)
			SELECT ch.event_id, ch.seat_id, ch.sector_id, ch.state, b.version, ch.hold_id, now()
			FROM changed ch JOIN bumped b USING (event_id, sector_id)
			WHERE ch.state IN ('held', 'sold')
			ON CONFLICT (event_id, seat_id) DO UPDATE
			SET state = EXCLUDED.state, version = EXCLUDED.version, hold_id = EXCLUDED.hold_id, updated_at = now()
			WHERE EXCLUDED.state = 'sold' OR catalog_seat_state.state <> 'sold'
			RETURNING event_id, seat_id
		),
		all_resolved AS (
			SELECT DISTINCT i.event_id, i.seat_id, s.sector_id FROM input i JOIN seats s ON s.id = i.seat_id
		)
		SELECT DISTINCT a.event_id, a.sector_id, coalesce(b.version, v.version) AS version FROM all_resolved a
		LEFT JOIN bumped b USING (event_id, sector_id)
		LEFT JOIN catalog_sector_version v USING (event_id, sector_id)`, payload)
	if err != nil {
		return nil, fmt.Errorf("catalog/pg: apply seat state batch: %w", err)
	}
	defer rows.Close()
	var out []domain.SectorVersion
	for rows.Next() {
		var v domain.SectorVersion
		if err := rows.Scan(&v.EventID, &v.SectorID, &v.Version); err != nil {
			return nil, fmt.Errorf("catalog/pg: scan sector version: %w", err)
		}
		out = append(out, v)
	}
	return out, rows.Err()
}

// VenueExists — есть ли площадка.
func (r *Repo) VenueExists(ctx context.Context, id uuid.UUID) (bool, error) {
	var ok bool
	if err := r.db.QueryRow(ctx, "/* catalog.venue_exists */ SELECT EXISTS (SELECT 1 FROM venues WHERE id = $1)", id).Scan(&ok); err != nil {
		return false, fmt.Errorf("catalog/pg: venue exists: %w", err)
	}
	return ok, nil
}

// InsertEvent — новое мероприятие.
func (r *Repo) InsertEvent(ctx context.Context, e domain.Event) error {
	_, err := r.db.Exec(ctx, `/* catalog.insert_event */
		INSERT INTO events (id, name, venue_id, starts_at, sales_open_at, sales_state)
		VALUES ($1, $2, $3, $4, $5, $6)`, e.ID, e.Name, e.VenueID, e.StartsAt, e.SalesOpenAt, e.SalesState)
	if err != nil {
		return fmt.Errorf("catalog/pg: insert event: %w", err)
	}
	return nil
}

// UpdateSalesState — смена состояния продаж.
func (r *Repo) UpdateSalesState(ctx context.Context, id uuid.UUID, state api.SalesState) error {
	tag, err := r.db.Exec(ctx, "/* catalog.update_sales_state */ UPDATE events SET sales_state = $2 WHERE id = $1", id, state)
	if err != nil {
		return fmt.Errorf("catalog/pg: update sales state: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return api.ErrEventNotFound
	}
	return nil
}
