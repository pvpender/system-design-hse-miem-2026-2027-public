package domain

import (
	"errors"
	"testing"

	"github.com/nikolaysavelev/soldout/internal/booking/api"
)

func TestPriceForSequence(t *testing.T) {
	tests := []struct {
		name     string
		sequence int64
		want     int64
	}{
		{name: "первый номер", sequence: 1, want: 500000},
		{name: "последний номер базовой ступени", sequence: 100, want: 500000},
		{name: "первый номер второй ступени", sequence: 101, want: 550000},
		{name: "последний номер второй ступени", sequence: 200, want: 550000},
		{name: "третья ступень", sequence: 201, want: 605000},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := PriceForSequence(500000, 10, 100, tt.sequence)
			if err != nil || got != tt.want {
				t.Fatalf("PriceForSequence(%d) = %d, %v; ожидалось %d", tt.sequence, got, err, tt.want)
			}
		})
	}
}

func TestPriceForSequence_Rounding(t *testing.T) {
	got, err := PriceForSequence(999, 10, 1, 2)
	if err != nil || got != 1099 {
		t.Fatalf("цена с округлением = %d, %v; ожидалось 1099", got, err)
	}
}

func TestPriceForSequence_Validation(t *testing.T) {
	for _, args := range []struct {
		base      int64
		percent   int
		step, seq int64
	}{{0, 10, 100, 1}, {100, -1, 100, 1}, {100, 10, 0, 1}, {100, 10, 100, 0}} {
		if _, err := PriceForSequence(args.base, args.percent, args.step, args.seq); !errors.Is(err, api.ErrValidation) {
			t.Errorf("PriceForSequence(%v) должна отклонить параметры: %v", args, err)
		}
	}
}
