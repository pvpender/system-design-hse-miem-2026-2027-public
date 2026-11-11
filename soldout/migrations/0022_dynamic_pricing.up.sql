-- 0022_dynamic_pricing: настройки цены сектора и зафиксированные номер и цена hold

ALTER TABLE sectors
    ADD COLUMN base_price_minor bigint NOT NULL DEFAULT 500000 CHECK (base_price_minor > 0),
    ADD COLUMN price_increase_percent int NOT NULL DEFAULT 10 CHECK (price_increase_percent >= 0),
    ADD COLUMN price_step_size bigint NOT NULL DEFAULT 100 CHECK (price_step_size > 0);

ALTER TABLE holds
    ADD COLUMN sector_id uuid,
    ADD COLUMN price_sequence bigint,
    ADD COLUMN price_minor bigint;

WITH numbered AS (
    SELECT h.id, s.sector_id,
           row_number() OVER (PARTITION BY s.sector_id ORDER BY h.created_at, h.id) AS price_sequence,
           sec.base_price_minor, sec.price_increase_percent, sec.price_step_size
    FROM holds h
    JOIN seats s ON s.id = h.seat_id
    JOIN sectors sec ON sec.id = s.sector_id
)
UPDATE holds h
SET sector_id = n.sector_id,
    price_sequence = n.price_sequence,
    price_minor = round(n.base_price_minor * power(
        1 + n.price_increase_percent::numeric / 100,
        floor((n.price_sequence - 1)::numeric / n.price_step_size)
    ))::bigint
FROM numbered n
WHERE n.id = h.id;

ALTER TABLE holds
    ALTER COLUMN sector_id SET NOT NULL,
    ALTER COLUMN price_sequence SET NOT NULL,
    ALTER COLUMN price_minor SET NOT NULL,
    ADD CONSTRAINT holds_price_minor_positive CHECK (price_minor > 0),
    ADD CONSTRAINT holds_price_sequence_positive CHECK (price_sequence > 0);

CREATE UNIQUE INDEX holds_sector_price_sequence_uidx ON holds (sector_id, price_sequence);
