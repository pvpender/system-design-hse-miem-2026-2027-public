# soldout — сквозная система курса

Сервис продажи билетов на «дроп»: 40 000 мест, 2 000 000 желающих, старт продаж в 12:00. На теге `lesson-1` — модульный монолит на Go 1.27 с PostgreSQL 17 и Valkey 9.

## Пять команд

```bash
make up          # собрать образ, поднять postgres + valkey + soldout, дождаться /readyz (≤ 60 с)
make smoke       # k6: 10 VU × 60 с — seatmap → queue/join → hold → order → pay; должен быть зелёным
make invariants  # инвариант-чекер I1–I4 по данным PostgreSQL; должен быть зелёным
make lint-arch   # go-arch-lint: правило зависимостей модулей (fitness function)
make test        # go test ./... — unit-тесты домена + интеграционный (testcontainers, нужен Docker)
```

Дополнительно: `make storm` (0→1000 VU, ожидаемо **красный** по p99 — стартовая точка занятия 2), `make hot-row`, `make up-lite` (seed 3×4000 мест), `make up-obs` (Prometheus + Grafana), `make down`.

## Что где

| Путь | Назначение |
|---|---|
| `cmd/soldout/main.go` | сборка зависимостей, HTTP-сервер, graceful shutdown |
| `internal/<module>/{api,domain,app,adapters}` | модули `catalog`, `queue`, `booking`, `payment`, `ticketing`, `notification` |
| `internal/platform/` | config, db (pgx + golang-migrate), cache (valkey-go), httpx, log, metrics, faults, outbox (пусто до L3) |
| `migrations/`, `seed/` | SQL-миграции (по модулям) и seed 3 × 40 000 мест; встроены в бинарник |
| `api/openapi.yaml` | контракт HTTP API |
| `spec/soldout.md` | спецификация системы (ФТ, НФТ, расчёт нагрузки, модули, инварианты) |
| `docs/constitution.md`, `AGENTS.md`, `CLAUDE.md` | правила для людей и агентов |
| `docs/adr/ADR-001-modular-monolith.md` | решение о стартовой архитектуре и известные ограничения MVP |
| `.go-arch-lint.yml` | архитектурные правила (`make lint-arch`) |
| `k6/` | `smoke`, `storm`, `hot-row`, `waiting-room` |
| `tools/invariant-checker/` | проверка инвариантов I1–I4 (I5 — с занятия 3) |
| `docker-compose.yml`, `compose/` | стенд; оверлеи `lesson-01.yml`, `lite.yml`; профили `load`, `tools`, `obs` |

## Правило зависимостей (кратко)

Модуль импортирует другой модуль только через его пакет `api`. Разрешённые рёбра: `booking → catalog.api, queue.api`; `payment → booking.api`; `ticketing → booking.api`; `notification → ticketing.api`; `queue → catalog.api`; `catalog → booking.api` (статусы мест и sold_count). `platform` не знает о бизнес-модулях. Проверяется `go-arch-lint`.

## Быстрая проверка руками

```bash
EVENT=10000000-0000-4000-8000-000000000001; USER=$(uuidgen | tr A-Z a-z)
TOKEN=$(curl -s -XPOST localhost:8080/v1/events/$EVENT/queue/join -d "{\"user_id\":\"$USER\"}" | jq -r .token)
HOLD=$(curl -s -XPOST localhost:8080/v1/holds -H "X-Admission-Token: $TOKEN" -d "{\"event_id\":\"$EVENT\",\"seat_id\":1,\"user_id\":\"$USER\"}" | jq -r .id)
ORDER=$(curl -s -XPOST localhost:8080/v1/orders -H "Idempotency-Key: $(uuidgen)" -d "{\"hold_ids\":[\"$HOLD\"],\"user_id\":\"$USER\"}" | jq -r .id)
curl -s -XPOST localhost:8080/v1/orders/$ORDER/pay | jq .status
curl -s localhost:8080/v1/users/$USER/tickets | jq .
```

## Известные ограничения (намеренно, см. ADR-001)

Нет индекса `holds(seat_id) WHERE status='active'`; статусы мест для карты зала читаются по `holds(event_id, status)` без индекса; пул pgx `MaxConns=200` при `max_connections=100`; карта зала (40 000 мест) сериализуется целиком на каждый запрос; выпуск билета и уведомление — синхронно внутри `pay`; блокировка места — advisory-lock без try-lock. Всё это чинится на занятиях 2–3.
