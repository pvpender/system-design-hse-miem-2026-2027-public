-- seed.sql: 3 мероприятия, у каждого своя площадка на N мест (N = soldout.seats_per_event, по умолчанию 40 000).
-- Площадка = N/1000 секторов по 1000 мест: все, кроме двух последних, — seated (20 рядов × 50 мест),
-- два последних — standing (танцпол, row_no = 0). Идентификаторы детерминированы, чтобы k6 и demo.md
-- могли ссылаться на них без запроса каталога:
--   event i  : 10000000-0000-4000-8000-00000000000i
--   venue i  : 20000000-0000-4000-8000-00000000000i
--   seat id  : (i-1)*N + (sector_idx-1)*1000 + номер места в секторе (1..1000)
-- Скрипт идемпотентен относительно повторного запуска только при пустой таблице events (проверяет приложение).

DO $$
DECLARE
    n_seats     int := coalesce(nullif(current_setting('soldout.seats_per_event', true), ''), '40000')::int;
    n_sectors   int;
    ev          int;
    sec         int;
    venue_id    uuid;
    sector_id   uuid;
    base        bigint;
    kind        text;
BEGIN
    IF n_seats < 3000 OR n_seats % 1000 <> 0 THEN
        RAISE EXCEPTION 'soldout.seats_per_event должно быть кратно 1000 и >= 3000, получено %', n_seats;
    END IF;
    n_sectors := n_seats / 1000;

    FOR ev IN 1..3 LOOP
        venue_id := ('20000000-0000-4000-8000-' || lpad(to_hex(ev), 12, '0'))::uuid;
        INSERT INTO venues (id, name) VALUES (venue_id, 'Арена ' || ev);

        FOR sec IN 1..n_sectors LOOP
            sector_id := ('30000000-0000-4000-8000-' || lpad(to_hex(ev * 1000 + sec), 12, '0'))::uuid;
            base := (ev - 1)::bigint * n_seats + (sec - 1)::bigint * 1000;
            kind := CASE WHEN sec > n_sectors - 2 THEN 'standing' ELSE 'seated' END;
            INSERT INTO sectors (id, venue_id, name, kind, capacity, base_price_minor, price_increase_percent, price_step_size)
            VALUES (sector_id, venue_id,
                    CASE WHEN kind = 'standing' THEN 'Танцпол ' || (sec - (n_sectors - 2)) ELSE 'Сектор ' || sec END,
                    kind, 1000, 500000, 10, 100);
            IF kind = 'seated' THEN
                INSERT INTO seats (id, sector_id, row_no, seat_no)
                SELECT base + (r - 1) * 50 + s, sector_id, r, s
                FROM generate_series(1, 20) AS r, generate_series(1, 50) AS s;
            ELSE
                INSERT INTO seats (id, sector_id, row_no, seat_no)
                SELECT base + s, sector_id, 0, s
                FROM generate_series(1, 1000) AS s;
            END IF;
        END LOOP;

        INSERT INTO events (id, name, venue_id, starts_at, sales_open_at, sales_state)
        VALUES (('10000000-0000-4000-8000-' || lpad(to_hex(ev), 12, '0'))::uuid,
                CASE ev WHEN 1 THEN 'Единственный концерт в Москве'
                        WHEN 2 THEN 'Дополнительный концерт'
                        ELSE 'Резервная дата' END,
                venue_id,
                now() + (ev * 30 || ' days')::interval,
                now() - interval '1 hour',
                CASE WHEN ev = 3 THEN 'scheduled' ELSE 'open' END);
    END LOOP;
END $$;

ANALYZE venues; ANALYZE sectors; ANALYZE seats; ANALYZE events;
