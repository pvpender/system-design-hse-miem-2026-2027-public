// Package pg — PostgreSQL-хранилище каталога: venues, sectors, seats, events и проекция карты зала
// (catalog_seat_state, catalog_sector_version). Таблицы других модулей не читаются.
package pg

import (
	"context"
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

// ApplySeatState — проекция + версия сектора в одной транзакции. free → строка удаляется.
func (r *Repo) ApplySeatState(ctx context.Context, eventID uuid.UUID, seatID int64, state api.SeatStatus) (uuid.UUID, int64, error) {
	var sectorID uuid.UUID
	var version int64
	err := db.WithTx(ctx, r.pool, func(tx pgx.Tx) error {
		if err := tx.QueryRow(ctx, "/* catalog.seat_sector */ SELECT sector_id FROM seats WHERE id = $1", seatID).Scan(&sectorID); err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				return api.ErrSeatNotFound
			}
			return fmt.Errorf("catalog/pg: seat sector: %w", err)
		}
		if err := tx.QueryRow(ctx, `/* catalog.bump_sector_version */
			INSERT INTO catalog_sector_version (event_id, sector_id, version, updated_at) VALUES ($1, $2, 1, now())
			ON CONFLICT (event_id, sector_id) DO UPDATE SET version = catalog_sector_version.version + 1, updated_at = now()
			RETURNING version`, eventID, sectorID).Scan(&version); err != nil {
			return fmt.Errorf("catalog/pg: bump version: %w", err)
		}
		if state == api.SeatFree {
			_, err := tx.Exec(ctx, "/* catalog.projection_delete */ DELETE FROM catalog_seat_state WHERE event_id = $1 AND seat_id = $2", eventID, seatID)
			if err != nil {
				return fmt.Errorf("catalog/pg: projection delete: %w", err)
			}
			return nil
		}
		_, err := tx.Exec(ctx, `/* catalog.projection_upsert */
			INSERT INTO catalog_seat_state (event_id, seat_id, sector_id, state, version, updated_at) VALUES ($1, $2, $3, $4, $5, now())
			ON CONFLICT (event_id, seat_id) DO UPDATE SET state = EXCLUDED.state, version = EXCLUDED.version, updated_at = now()`,
			eventID, seatID, sectorID, state, version)
		if err != nil {
			return fmt.Errorf("catalog/pg: projection upsert: %w", err)
		}
		return nil
	})
	return sectorID, version, err
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
