// Package valkey — атомарное резервирование порядкового номера и цены сектора.
package valkey

import (
	"context"
	"fmt"
	"strconv"

	"github.com/google/uuid"
	vk "github.com/valkey-io/valkey-go"

	"github.com/nikolaysavelev/soldout/internal/booking/app"
	"github.com/nikolaysavelev/soldout/internal/booking/domain"
	"github.com/nikolaysavelev/soldout/internal/platform/otel"
)

const reservePriceScript = `
local sequence = redis.call('INCR', KEYS[1])
local level = math.floor((sequence - 1) / tonumber(ARGV[3]))
local price = math.floor(tonumber(ARGV[1]) * math.pow(1 + tonumber(ARGV[2]) / 100, level) + 0.5)
return {sequence, price}`

// Pricer — реализация app.PriceReserver через один Lua-вызов Valkey.
type Pricer struct {
	client  vk.Client
	reserve *vk.Lua
}

// New создаёт калькулятор динамической цены.
func New(client vk.Client) *Pricer {
	return &Pricer{client: client, reserve: vk.NewLuaScript(reservePriceScript)}
}

var _ app.PriceReserver = (*Pricer)(nil)

func key(sectorID uuid.UUID) string { return "pricing:sector:" + sectorID.String() + ":sequence" }

// ReservePrice атомарно увеличивает счётчик сектора и возвращает цену полученного номера.
func (p *Pricer) ReservePrice(ctx context.Context, sectorID uuid.UUID, basePriceMinor int64, increasePercent int, stepSize int64) (reservation domain.PriceReservation, err error) {
	ctx, span := otel.Start(ctx, "valkey.ReservePrice", otel.String("sector.id", sectorID.String()))
	defer otel.End(span, &err)
	result := p.reserve.Exec(ctx, p.client, []string{key(sectorID)}, []string{
		strconv.FormatInt(basePriceMinor, 10), strconv.Itoa(increasePercent), strconv.FormatInt(stepSize, 10),
	})
	if err := result.Error(); err != nil {
		return reservation, fmt.Errorf("booking/valkey: reserve price: %w", err)
	}
	values, err := result.ToArray()
	if err != nil || len(values) != 2 {
		return reservation, fmt.Errorf("booking/valkey: reserve price shape: %w", err)
	}
	sequence, err := values[0].ToInt64()
	if err != nil {
		return reservation, fmt.Errorf("booking/valkey: price sequence: %w", err)
	}
	price, err := values[1].ToInt64()
	if err != nil {
		return reservation, fmt.Errorf("booking/valkey: reserved price: %w", err)
	}
	return domain.PriceReservation{Sequence: sequence, PriceMinor: price}, nil
}
