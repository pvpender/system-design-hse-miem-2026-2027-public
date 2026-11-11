-- SeatStateChanged: независимый at-least-once polling-канал поверх transactional outbox.
ALTER TABLE outbox ADD COLUMN seq bigint GENERATED ALWAYS AS IDENTITY;
CREATE INDEX outbox_type_seq_idx ON outbox (aggregatetype, seq);

-- Дедупликация при повторной доставке после commit проекции, но до ack.
CREATE TABLE catalog_processed_events (
    id uuid PRIMARY KEY
);

-- High-watermark отдельно от проекции: free удаляет строку catalog_seat_state,
-- но позднее старое событие всё равно должно быть отброшено.
CREATE TABLE catalog_seat_projection_position (
    event_id        uuid   NOT NULL,
    seat_id         bigint NOT NULL,
    last_source_seq bigint NOT NULL,
    PRIMARY KEY (event_id, seat_id)
);
