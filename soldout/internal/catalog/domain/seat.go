package domain

import (
	"sort"

	"github.com/google/uuid"

	"github.com/nikolaysavelev/soldout/internal/catalog/api"
)

// Sector — сектор площадки.
type Sector struct {
	ID       uuid.UUID
	VenueID  uuid.UUID
	Name     string
	Kind     api.SectorKind
	Capacity int
}

// Seat — место.
type Seat struct {
	ID       int64
	SectorID uuid.UUID
	VenueID  uuid.UUID
	RowNo    int
	SeatNo   int
	Pricing  api.SectorPricing
}

// ToAPI — DTO.
func (s Seat) ToAPI() api.Seat {
	return api.Seat{ID: s.ID, SectorID: s.SectorID, VenueID: s.VenueID, RowNo: s.RowNo, SeatNo: s.SeatNo, Pricing: s.Pricing}
}

// SeatState — строка проекции: место сектора со статусом held/sold.
type SeatState struct {
	SeatID int64
	Status api.SeatStatus
}

// SeatStateChange — событие outbox для batch-проекции.
type SeatStateChange struct {
	ID        uuid.UUID      `json:"id"`
	SourceSeq int64          `json:"source_seq"`
	EventID   uuid.UUID      `json:"event_id"`
	SeatID    int64          `json:"seat_id"`
	State     api.SeatStatus `json:"state"`
}

// SectorVersion — текущая версия сектора после batch (нужна и при повторной инвалидации).
type SectorVersion struct {
	EventID  uuid.UUID
	SectorID uuid.UUID
	Version  int64
}

// BuildSectorSeatMap собирает карту одного сектора: все места сектора, статусы из проекции, остальные — free.
func BuildSectorSeatMap(eventID uuid.UUID, sec Sector, seats []Seat, states []SeatState, version int64) api.SeatMapSector {
	byID := make(map[int64]api.SeatStatus, len(states))
	for _, st := range states {
		byID[st.SeatID] = st.Status
	}
	out := api.SeatMapSector{EventID: eventID, ID: sec.ID, Name: sec.Name, Kind: sec.Kind, Version: version, Seats: make([]api.SeatMapSeat, 0, len(seats))}
	sorted := append([]Seat(nil), seats...)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i].ID < sorted[j].ID })
	for _, s := range sorted {
		st, ok := byID[s.ID]
		if !ok {
			st = api.SeatFree
		}
		out.Seats = append(out.Seats, api.SeatMapSeat{ID: s.ID, RowNo: s.RowNo, SeatNo: s.SeatNo, Status: st})
	}
	return out
}

// SectorCounts — счётчики held/sold сектора из проекции.
type SectorCounts struct {
	Held, Sold int
	Version    int64
}

// BuildSummary собирает сводку по секторам: total из capacity, free = total − held − sold.
func BuildSummary(eventID uuid.UUID, sectors []Sector, counts map[uuid.UUID]SectorCounts) api.SeatMap {
	out := api.SeatMap{EventID: eventID, Sectors: make([]api.SectorSummary, 0, len(sectors))}
	sorted := append([]Sector(nil), sectors...)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i].Name < sorted[j].Name })
	for _, sec := range sorted {
		c := counts[sec.ID]
		free := sec.Capacity - c.Held - c.Sold
		if free < 0 {
			free = 0
		}
		out.Sectors = append(out.Sectors, api.SectorSummary{ID: sec.ID, Name: sec.Name, Kind: sec.Kind, Total: sec.Capacity, Free: free, Held: c.Held, Sold: c.Sold, Version: c.Version})
	}
	return out
}
