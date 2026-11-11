package kafka

import (
	"testing"

	"github.com/twmb/franz-go/pkg/kgo"
)

// Водяная отметка не проходит незавершённое сообщение: коммит до первого «висящего» смещения.
func TestTrackerWatermark(t *testing.T) {
	tr := newTracker()
	rec := func(off int64) *kgo.Record { return &kgo.Record{Partition: 0, Offset: off} }
	for off := int64(10); off <= 13; off++ {
		tr.start(rec(off))
	}
	steps := []struct {
		done int64
		want int64
	}{
		{11, 10}, // 10 ещё идёт (медленное письмо) — коммитить можно только до 10
		{13, 10},
		{10, 12}, // 10 и 11 готовы, 12 идёт
		{12, 14}, // всё готово — следующее смещение 14
	}
	for _, s := range steps {
		got, _ := tr.finish(rec(s.done))
		if got != s.want {
			t.Fatalf("после %d: отметка %d, ожидали %d", s.done, got, s.want)
		}
	}
}
