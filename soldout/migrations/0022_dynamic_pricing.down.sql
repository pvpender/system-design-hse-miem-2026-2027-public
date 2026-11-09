DROP INDEX holds_sector_price_sequence_uidx;

ALTER TABLE holds
    DROP CONSTRAINT holds_price_sequence_positive,
    DROP CONSTRAINT holds_price_minor_positive,
    DROP COLUMN price_minor,
    DROP COLUMN price_sequence,
    DROP COLUMN sector_id;

ALTER TABLE sectors
    DROP COLUMN price_step_size,
    DROP COLUMN price_increase_percent,
    DROP COLUMN base_price_minor;
