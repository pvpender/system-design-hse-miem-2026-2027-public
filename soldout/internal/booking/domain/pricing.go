package domain

import (
	"fmt"
	"math"

	"github.com/nikolaysavelev/soldout/internal/booking/api"
)

// PriceReservation — атомарно выделенные порядковый номер и цена места в секторе.
type PriceReservation struct {
	Sequence   int64
	PriceMinor int64
}

// PriceForSequence вычисляет цену одно-based номера: каждые stepSize номеров цена растёт на increasePercent.
func PriceForSequence(basePriceMinor int64, increasePercent int, stepSize, sequence int64) (int64, error) {
	if basePriceMinor <= 0 || increasePercent < 0 || stepSize <= 0 || sequence <= 0 {
		return 0, fmt.Errorf("%w: некорректные параметры ценообразования", api.ErrValidation)
	}
	level := (sequence - 1) / stepSize
	price := math.Round(float64(basePriceMinor) * math.Pow(1+float64(increasePercent)/100, float64(level)))
	if price > math.MaxInt64 {
		return 0, fmt.Errorf("%w: цена превышает допустимый диапазон", api.ErrValidation)
	}
	return int64(price), nil
}
