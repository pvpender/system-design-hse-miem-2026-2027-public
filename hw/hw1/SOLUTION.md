# Отчёт по HW1

## Ссылки

- [Спецификация](../../soldout/spec/soldout-V2.md)
- [ADR-101: динамическое ценообразование](../../soldout/docs/adr/ADR-101-V2.md)
- [k6 storm до](../../soldout/k6/results/BACKUP/storm-hw1-solution.json) / [после](../../soldout/k6/results/storm-hw1-solution.json)
- [k6 hot-row до](../../soldout/k6/results/BACKUP/hot-row-hw1-solution.json) / [после](../../soldout/k6/results/hot-row-hw1-solution.json)
- [CPU-профиль до](../../soldout/k6/results/BACKUP/pprof-top-hw1-solution.txt) / [после](../../soldout/k6/results/pprof-top-hw1-solution.txt)
- [Flame graph до](../../soldout/k6/results/BACKUP/pyroscope-hw1-solution.json) / [после](../../soldout/k6/results/pyroscope-hw1-solution.json)

## Что реализовано

### Динамическое ценообразование

Выбран вариант со ступенчатым ростом цены: настройки сектора `base_price_minor`, `price_increase_percent`, `price_step_size`; цена фиксируется при hold и далее не меняется.

- `migrations/0022_dynamic_pricing.up.sql` — настройки цены, сохранённые `price_sequence`/`price_minor`, ограничения корректности и уникальности номера в секторе.
- `internal/booking/domain/pricing.go` — детерминированный расчёт цены от sequence и проверки границ int64.
- `internal/booking/adapters/valkey/pricing.go` — Lua-скрипт атомарно делает `INCR` и вычисляет цену; переполнение преобразуется в `ErrPriceOverflow` и HTTP 409 вместо записи MinInt64 в PostgreSQL.
- `internal/booking/app/service.go` — сохранение зафиксированных sequence и цены в hold.
- `tools/invariant-checker/main.go` — новый инвариант **I6**: сумма заказа равна сумме цен hold, номера уникальны в секторе, цена соответствует формуле и место относится к правильному сектору.
- Integration-тесты проверяют 100 конкурентных ReservePrice и границы переполнения sequence 32301/40259.

### Оптимизация 1: Valkey-only admission

Раньше каждый Join делал `INSERT admissions` в PostgreSQL и затем `SET` в Valkey; Validate при промахе читал PostgreSQL. Теперь короткоживущий admission хранится только в Valkey с TTL.

Изменены `internal/queue/app/service.go`, `internal/queue/adapters/valkey/cache.go`, `cmd/soldout/main.go`; PG-адаптер queue удалён. Ошибка SET не возвращает непроверяемый токен, ошибка GET не маскируется под ложный 401. Таблица admissions оставлена для безопасного отката, но из горячего пути удалена.

### Оптимизация 2: transactional outbox и batch-проекция карты

Раньше SeatStateChanged публиковался после commit в in-process eventbus: при переполнении события отбрасывались, а I7 нарушался. Теперь booking пишет held/free/sold в outbox в той же транзакции, что и hold/release/pay/expire.

- `internal/booking/api/api.go`, `internal/booking/app/service.go`, `internal/booking/adapters/pg/store.go` — transactional append.
- `internal/platform/outbox/outbox.go` — batch INSERT, монотонный `seq`, фильтрация consumer по aggregate type.
- `internal/catalog/adapters/pg/repo.go` — set-based batch Apply, дедупликация и high-watermark по `(event, seat)`.
- `internal/catalog/app/service.go` — одна инвалидация кэша на сектор за batch; ack только после Apply и инвалидации.
- `migrations/0023_seat_state_outbox.*.sql` — `outbox.seq`, таблицы дедупликации и позиции проекции.
- `cmd/notifier/main.go` — polling notifier ограничен `order`, поэтому не забирает seat-события.

Повторная доставка идемпотентна; позднее старое событие после free не воскрешает место. После storm outbox полностью догнал нагрузку, I1–I7 зелёные.

## Измерения до/после

«До» — файлы в `k6/results/BACKUP`, «после» — одноимённые сохранённые файлы в `k6/results`. Повторные прогоны для отчёта не выполнялись. Ожидаемые 409 не считаются HTTP-ошибками: `http_req_failed` в обоих случаях равен 0.

### Storm

| Метрика | До | После | Изменение |
|---|---:|---:|---:|
| hold avg | 1114,5 мс | 876,3 мс | **−21,4%** |
| hold p50 | 1129,4 мс | 888,2 мс | **−21,4%** |
| hold p95 | 1586,0 мс | 1267,0 мс | **−20,1%** |
| hold p99 | 1882,1 мс | 1497,3 мс | **−20,4%** |
| hold max | 2317,8 мс | 1759,1 мс | **−24,1%** |
| seatmap p99 | 66,1 мс | 34,6 мс | **−47,6%** |
| Итерации за прогон | 88 293 | 139 984 | **+58,5%** |
| Итераций/с | 630,6 | 999,9 | **+58,5%** |
| Ошибки HTTP | 0% | 0% | без изменений |

Порог storm hold p99 < 500 мс всё ещё не достигнут; зафиксированное улучшение не выдаётся за выполнение целевого SLO.

### Hot-row

| Метрика | До | После | Изменение |
|---|---:|---:|---:|
| hold avg | 212,9 мс | 144,1 мс | **−32,4%** |
| hold p50 | 206,7 мс | 139,7 мс | **−32,5%** |
| hold p95 | 298,0 мс | 205,2 мс | **−31,1%** |
| hold p99 | 356,9 мс | 255,4 мс | **−28,4%** |
| hold max | 507,5 мс | 429,7 мс | **−15,3%** |
| Итерации за прогон | 70 758 | 133 038 | **+88,0%** |
| Итераций/с | 1175,6 | 2212,7 | **+88,2%** |

### SQL после оптимизации

Топ `pg_stat_statements` по `total_exec_time` после hot-row:

| Запрос | Calls | Total, мс | Mean, мс |
|---|---:|---:|---:|
| `catalog.get_seat` | 133 038 | 25 944,9 | 0,20 |
| `booking.count_active_holds` | 1 186 | 12 959,9 | 10,93 |
| статистика postgres-exporter | 4 744 | 5 694,9 | 4,80 |
| `catalog.get_event` | 266 076 | 4 335,5 | 0,02 |
| `pg_database_size` | 4 744 | 2 877,6 | 0,61 |

`queue.insert_admission` исчез из PostgreSQL. Для сопоставимого «до» в hot-row было 70 758 таких INSERT с суммарным временем 16 499,8 мс. `catalog.get_seat` ускорился с 0,31 до 0,20 мс в среднем; число вызовов выросло из-за почти двукратного роста итераций, а не из-за дополнительных запросов на итерацию.

Отдельный smoke подтвердил полный путь покупки: hold p99 30,9 мс, seatmap p95 8,2 мс, pay p99 344,9 мс, ошибок 0%. `make test`, `make lint-arch`, `make lint-migrations`, `make lint-semgrep` и `make invariants` зелёные.

### Отказ от третьей оптимизации

Не оставлен read-through кэш `catalog.GetEvent/GetSeat` со singleflight. Он должен был убрать повторные SQL, но A/B-показал смешанный результат в storm и сильную деградацию hot-row. Кроме того, write-through инвалидация была корректна только внутри одного процесса, а для нескольких реплик потребовала бы отдельного механизма.

По той же причине не оставлено объединение `CountUserHolds` и `SeatSold`: уменьшение числа round trips не дало устойчивого улучшения, а hot-row ухудшился.

## Как использовался ИИ

**Инструмент:** Kimi Code K3.1 Pro, включая чтение кодовой базы, генерацию Go/SQL/тестов, запуск проверок и разбор k6/Pyroscope.

**Что сделал агент:** сопоставил профиль с путями Join/CreateHold, реализовал динамическую цену и I6, предложил и реализовал Valkey-only admission и outbox batch-проекцию, добавил миграции, unit/integration-тесты и документацию, провёл проверки и первоначальные A/B-замеры.

**Что сделано руками:** выбор варианта и архитектурного направления, корректировки некоторых архитектурных решений, реализация ADR и спецификации, ревью и отклонение изменений приводящих к ухудшению метрик, контроль финальных сохранённых прогонов и выводов.

**Где агент ошибся**

1. Lua вычислял цену в double и при большом sequence возвращал MinInt64. Это обнаружилось по отказу constraint PostgreSQL; добавлены проверка границы, отдельная 409 `price_overflow` и регрессионные тесты.
2. Первые предложения, а именно кэш catalog и объединение SQL-проверок — ухудшили часть метрик. Это выявилось только повторными storm/hot-row, поэтому код вернут к состоянию до изменений.
3. В integration-тесте projector сначала был указан сектор `...0001` вместо фактического seed-сектора `...03e9`. Пустой результат был распознан как ошибку теста, а не SQL; после исправления ID проверки дедупликации и порядка прошли.
