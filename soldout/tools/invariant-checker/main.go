// Command invariant-checker проверяет инварианты домена soldout по данным PostgreSQL.
//
//	I1  нет двух active hold на одно (event, seat)
//	I2  нет двух tickets на одно (event, seat)
//	I3  у каждого paid заказа есть ticket на каждую позицию
//	I4  у пользователя ≤ 4 билетов на мероприятие
//	I5  (с занятия 3) каждому paid заказу соответствует ровно одна notification — включается флагом -i5
//	I6  сумма заказа равна сумме зафиксированных цен, номера и цены hold соответствуют сектору
//	I7  проекция карты совпадает с holds — управляется флагом -projection
//
// Выход: таблица; код возврата 1 при нарушении.
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"text/tabwriter"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

type invariant struct {
	id, name, sql string
}

var invariants = []invariant{
	{"I1", "нет двух active hold на одно (event, seat)", `
		SELECT count(*) FROM (
			SELECT event_id, seat_id FROM holds WHERE status = 'active' GROUP BY event_id, seat_id HAVING count(*) > 1
		) d`},
	{"I2", "нет двух tickets на одно (event, seat)", `
		SELECT count(*) FROM (
			SELECT o.event_id, t.seat_id FROM tickets t JOIN orders o ON o.id = t.order_id
			GROUP BY o.event_id, t.seat_id HAVING count(*) > 1
		) d`},
	{"I3", "у каждого paid заказа есть ticket на каждую позицию", `
		SELECT count(*) FROM orders o
		JOIN order_items oi ON oi.order_id = o.id
		JOIN holds h ON h.id = oi.hold_id
		LEFT JOIN tickets t ON t.order_id = o.id AND t.seat_id = h.seat_id
		WHERE o.status = 'paid' AND t.id IS NULL`},
	{"I4", "у пользователя ≤ 4 билетов на мероприятие", `
		SELECT count(*) FROM (
			SELECT o.user_id, o.event_id FROM tickets t JOIN orders o ON o.id = t.order_id
			GROUP BY o.user_id, o.event_id HAVING count(*) > 4
		) d`},
}

var invariantI6 = invariant{"I6", "цены и номера hold соответствуют сектору, сумма заказа равна сумме цен hold", `
		SELECT
			(SELECT count(*) FROM (
				SELECT o.id FROM orders o
				JOIN order_items oi ON oi.order_id = o.id
				JOIN holds h ON h.id = oi.hold_id
				GROUP BY o.id, o.amount HAVING o.amount * 100 <> sum(h.price_minor)
			) wrong_amount)
		+	(SELECT count(*) FROM (
				SELECT sector_id, price_sequence FROM holds
				GROUP BY sector_id, price_sequence HAVING count(*) > 1
			) duplicate_sequence)
		+	(SELECT count(*) FROM holds h JOIN seats st ON st.id = h.seat_id
				WHERE h.sector_id <> st.sector_id)
		+	(SELECT count(*) FROM holds h JOIN sectors s ON s.id = h.sector_id
				WHERE h.price_minor <> round(s.base_price_minor * power(
					1 + s.price_increase_percent::numeric / 100,
					floor((h.price_sequence - 1)::numeric / s.price_step_size)
				))::bigint)`}

var invariantI7 = invariant{"I7", "проекция карты (catalog_seat_state) совпадает с истиной (holds) — с занятия 2", `
		SELECT (SELECT count(*) FROM (
			SELECT h.event_id, h.seat_id, CASE h.status WHEN 'confirmed' THEN 'sold' ELSE 'held' END AS state
			FROM holds h WHERE h.status IN ('active','confirmed')
			EXCEPT
			SELECT event_id, seat_id, state FROM catalog_seat_state) missing)
		+ (SELECT count(*) FROM (
			SELECT event_id, seat_id, state FROM catalog_seat_state
			EXCEPT
			SELECT h.event_id, h.seat_id, CASE h.status WHEN 'confirmed' THEN 'sold' ELSE 'held' END
			FROM holds h WHERE h.status IN ('active','confirmed')) extra)`}

// I5 через агрегат и hash join: коррелированный подзапрос по payload->>'order_id' без индекса квадратичен
// и после checkout-storm (10 000+ заказов) упирается в statement_timeout.
var invariantI5 = invariant{"I5", "каждому paid заказу — ровно одна notification (с занятия 3)", `
		SELECT count(*) FROM orders o
		LEFT JOIN (
			SELECT payload->>'order_id' AS order_id, count(*) AS n
			FROM notifications WHERE kind = 'tickets_issued' GROUP BY 1
		) n ON n.order_id = o.id::text
		WHERE o.status = 'paid' AND coalesce(n.n, 0) <> 1`}

func main() {
	dsn := flag.String("dsn", os.Getenv("DATABASE_URL"), "строка подключения PostgreSQL (или DATABASE_URL)")
	withI5 := flag.Bool("i5", false, "проверять I5 (уведомления; актуально с занятия 3)")
	withI7 := flag.Bool("projection", true, "проверять I7: проекция карты зала совпадает с holds (занятие 2; проекция догоняет события за секунды)")
	flag.Parse()
	if *dsn == "" {
		fmt.Fprintln(os.Stderr, "нужен -dsn или DATABASE_URL")
		os.Exit(2)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	pool, err := connectWithRetry(ctx, *dsn, 30*time.Second)
	if err != nil {
		fmt.Fprintln(os.Stderr, "подключение:", err)
		os.Exit(2)
	}
	defer pool.Close()

	checks := append(invariants, invariantI6)
	if *withI7 {
		checks = append(checks, invariantI7)
	}
	if *withI5 {
		checks = append(checks, invariantI5)
	}

	tw := tabwriter.NewWriter(os.Stdout, 0, 4, 2, ' ', 0)
	fmt.Fprintln(tw, "ID\tИнвариант\tНарушений\tСтатус")
	violations := 0
	for _, inv := range checks {
		var n int64
		if err := pool.QueryRow(ctx, "/* invariants."+inv.id+" */ "+inv.sql).Scan(&n); err != nil {
			fmt.Fprintf(tw, "%s\t%s\t-\tОШИБКА: %v\n", inv.id, inv.name, err)
			violations++
			continue
		}
		status := "OK"
		if n > 0 {
			status = "НАРУШЕН"
			violations++
		}
		fmt.Fprintf(tw, "%s\t%s\t%d\t%s\n", inv.id, inv.name, n, status)
	}
	tw.Flush()

	var stats struct{ holds, confirmed, orders, paid, tickets int64 }
	statsErr := pool.QueryRow(ctx, `/* invariants.stats */ SELECT
		(SELECT count(*) FROM holds WHERE status = 'active'),
		(SELECT count(*) FROM holds WHERE status = 'confirmed'),
		(SELECT count(*) FROM orders),
		(SELECT count(*) FROM orders WHERE status = 'paid'),
		(SELECT count(*) FROM tickets)`).Scan(&stats.holds, &stats.confirmed, &stats.orders, &stats.paid, &stats.tickets)
	if statsErr != nil {
		fmt.Printf("\nстатистика недоступна: %v\n", statsErr)
	} else {
		fmt.Printf("\nactive holds=%d confirmed holds=%d orders=%d paid=%d tickets=%d\n", stats.holds, stats.confirmed, stats.orders, stats.paid, stats.tickets)
	}

	if violations > 0 {
		fmt.Printf("\nНАРУШЕНИЙ: %d\n", violations)
		os.Exit(1)
	}
	fmt.Println("\nВсе инварианты соблюдены.")
}

// connectWithRetry ждёт, пока PostgreSQL примет соединение: сразу после шторма все слоты
// max_connections могут быть заняты пулом приложения (ADR-001, ограничение 3).
func connectWithRetry(ctx context.Context, dsn string, maxWait time.Duration) (*pgxpool.Pool, error) {
	deadline := time.Now().Add(maxWait)
	for {
		pool, err := pgxpool.New(ctx, dsn)
		if err == nil {
			if err = pool.Ping(ctx); err == nil {
				return pool, nil
			}
			pool.Close()
		}
		if time.Now().After(deadline) {
			return nil, err
		}
		fmt.Fprintln(os.Stderr, "ожидание PostgreSQL:", err)
		time.Sleep(2 * time.Second)
	}
}
