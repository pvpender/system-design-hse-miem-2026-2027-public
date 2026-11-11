DROP TABLE IF EXISTS catalog_seat_projection_position;
DROP TABLE IF EXISTS catalog_processed_events;
DROP INDEX IF EXISTS outbox_type_seq_idx;
ALTER TABLE outbox DROP COLUMN IF EXISTS seq;
