# Архитектура soldout: верхний уровень и ключевые сценарии

Документ описывает код ветки `demo/l2-baseline` (это `lesson-1` на стенде занятия 2). Диаграммы пересказывают код: при расхождении верен код. Источники истины рядом: `spec/soldout.md` (требования и расчет нагрузки), `api/openapi.yaml` (контракт HTTP), `.go-arch-lint.yml` (правило зависимостей), `migrations/` (схема данных), `docs/constitution.md` (инварианты).

Задача системы: продать 40 000 мест на один концерт, старт продаж в 12:00, интерес порядка 2 000 000 человек. Ни одно место не должно быть продано дважды, ни один оплаченный заказ не должен потеряться.

## 1. Контекст

```mermaid
flowchart LR
    U[Покупатель, браузер]
    A[Администратор]
    OP[Оператор]
    K6[k6: smoke, storm, hot-row]

    subgraph S[soldout: один процесс Go 1.27]
        API[HTTP API v1, chi]
        MOD[Модули: catalog, queue, booking, payment, ticketing, notification]
        PSP[Эмулятор PSP: in-process]
        NOTIF[Эмулятор доставки: структурный лог]
        EXP[Expirer: фоновая горутина, 5 с]
    end

    PG[(PostgreSQL 17: источник истины)]
    VK[(Valkey 9: токены допуска)]
    OBS[Prometheus, Grafana, Pyroscope]
    CHK[invariant-checker: I1-I4]

    U -->|seatmap, queue/join, holds, orders, pay, tickets| API
    A -->|admin/events| API
    K6 --> API
    API --> MOD
    MOD --> PSP
    MOD --> NOTIF
    MOD --> PG
    EXP --> PG
    MOD --> VK
    API -->|/metrics, /debug/pprof| OBS
    OP --> OBS
    OP --> CHK
    CHK --> PG
```

Платежный провайдер и канал доставки уведомлений живут внутри процесса как эмуляторы с управляемыми отказами (`PSP_FAIL_RATE`, `PSP_DELAY_MS`). Внешних интеграций у системы нет: все, что выглядит как внешний мир, подменено адаптером.

## 2. Стенд

```mermaid
flowchart TB
    subgraph L[docker compose, профиль obs]
        K[k6, профиль load] --> SO[soldout :8080, 2 CPU, 512 МБ, GOMEMLIMIT 440MiB]
        SO --> PB[pgbouncer :6432, transaction pooling, pool 20]
        SO --> VK[(valkey :6379, 0.5 CPU)]
        PB --> PGS[(postgres :5432, 1 CPU, 1 ГБ, max_connections 100)]
        SO -. миграции и seed напрямую .-> PGS
        PROM[prometheus :9090] -->|scrape 5 с| SO
        PROM --> PGE[postgres-exporter]
        PROM --> VKE[valkey-exporter]
        AL[alloy: pull /debug/pprof каждые 15 с] --> SO
        AL --> PY[pyroscope :4040]
        GR[grafana :3000] --> PROM
        GR --> PY
        IC[invariant-checker, профиль tools] --> PGS
    end
```

Лимиты CPU и памяти заданы в `compose/lesson-02.yml`. Без них 12 ядер ноутбука прячут узкие места: стенд перестает воспроизводить прод в масштабе 1:100.

На baseline пул приложения ходит в PostgreSQL напрямую (`DB_MAX_CONNS=200` против `max_connections=100`), PgBouncer поднят и не используется: `/readyz` отдает `"pgbouncer":"direct"`. С шага A приложение подключается через пулер.

Миграции и seed всегда идут в PostgreSQL напрямую по `DATABASE_URL_DIRECT`: `golang-migrate` держит сессионный advisory-lock, несовместимый с transaction pooling, а `statement_timeout=0` нужен для `CREATE INDEX CONCURRENTLY`.

## 3. Модули и правило зависимостей

Модуль импортирует другой модуль только через его пакет `api` (DTO, ошибки, интерфейсы). Правило машиночитаемо и проверяется в CI: `make lint-arch`.

```mermaid
flowchart LR
    catalog[catalog]
    queue[queue]
    booking[booking]
    payment[payment]
    ticketing[ticketing]
    notification[notification]
    platform[platform: config, db, cache, httpx, log, metrics, faults]

    booking --> catalog
    booking --> queue
    queue --> catalog
    payment --> booking
    ticketing --> booking
    notification --> ticketing
    catalog -->|SeatStates, SoldCount: единственное ребро против потока| booking

    booking --- platform
    catalog --- platform
    queue --- platform
    payment --- platform
    ticketing --- platform
    notification --- platform
```

| Модуль | Ответственность | Владеет данными | HTTP |
|---|---|---|---|
| `catalog` | мероприятия, площадки, секторы, места, карта зала | `venues`, `sectors`, `seats`, `events` | `GET /v1/events`, `GET /v1/events/{id}`, `GET /v1/events/{id}/seatmap`, `POST /v1/admin/events`, `POST /v1/admin/events/{id}/open` |
| `queue` | допуск в waiting room, проверка токена | ключи Valkey `admission:{token}` с TTL | `POST /v1/events/{id}/queue/join` |
| `booking` | hold, release, заказ, оплата как сценарий, лимит 4, истечение | `holds`, `orders`, `order_items` | `POST /v1/holds`, `GET /v1/holds/{id}`, `DELETE /v1/holds/{id}`, `POST /v1/orders`, `GET /v1/orders/{id}`, `POST /v1/orders/{id}/pay` |
| `payment` | идемпотентное списание через эмулятор PSP | `payments` | нет, вызывается через порт |
| `ticketing` | выпуск билета с кодом, чтение билетов | `tickets` | `GET /v1/users/{id}/tickets` |
| `notification` | запись и доставка уведомлений | `notifications` | нет, вызывается через порт |
| `platform` | конфигурация, пул pgx, кэш, HTTP-обвязка, логи, метрики, fault-flags | `outbox` появится на занятии 3 | `/healthz`, `/readyz`, `/metrics`, `/debug/pprof` |

Сборка зависимостей целиком лежит в `cmd/soldout/main.go`: там `booking` получает `payment` и `ticketing` как реализации портов `PaymentGateway` и `TicketIssuer`, объявленных в `booking/api`. Направление импортов при этом остается прежним: `payment` знает про `booking.api`, обратного импорта нет.

## 4. Устройство одного модуля

```mermaid
flowchart LR
    HTTP[adapters/http: chi, парсинг, mapErr в коды] --> APP[app: сценарий, транзакционные границы]
    APP --> DOM[domain: правила, без БД и HTTP]
    APP --> PGA[adapters/pg: SQL, pgx]
    APP --> VKA[adapters/valkey, adapters/psp, adapters/log]
    APIP[api: DTO, ошибки, интерфейсы для других модулей]
    APP -. реализует .-> APIP
```

Слой `app` держит транзакционную границу: каждая мутация проходит через `Store.InTx`. Слой `domain` проверяет правила и не знает про БД. Ошибки домена и `api` превращаются в HTTP-коды в одном месте, функция `mapErr` в `adapters/http`.

## 5. Сценарии

### 5.1 Вход в waiting room и проверка допуска

Токен привязан к паре (event, user) и живет `ADMISSION_TTL` (15 минут). Valkey — единственное хранилище допуска; после потери Valkey пользователь повторно проходит Join.

```mermaid
sequenceDiagram
    autonumber
    actor U as Покупатель
    participant QH as queue/http
    participant QA as queue/app
    participant C as catalog.api
    participant VK as Valkey

    U->>QH: POST /v1/events/{id}/queue/join {user_id}
    QH->>QA: Join(eventID, userID)
    QA->>C: GetEvent(eventID)
    alt мероприятия нет
        C-->>QA: ErrEventNotFound
        QA-->>U: 404 event_not_found
    else есть
        QA->>QA: NewAdmission: token = 16 случайных байт, position = 0
        QA->>VK: SET admission:{token} JSON PX 15m
        Note over QA,VK: ошибка SET означает ошибку Join: непроверяемый токен не возвращается
        QA-->>U: 200 {token, position: 0, expires_at}
    end

    Note over U,VK: очереди на baseline нет, допуск выдается сразу. Настоящая очередь с admission rate появляется на шаге D занятия 2

    U->>QH: любой запрос с X-Admission-Token
    participant BA as booking/app
    BA->>QA: Validate(token, eventID, userID)
    QA->>VK: GET admission:{token}
    alt ключа нет
        QA-->>BA: ErrAdmissionInvalid, дальше 401 admission_required
    else ошибка Valkey
        QA-->>BA: инфраструктурная ошибка, не 401
    else токен найден
        QA->>QA: ValidFor: совпали event_id и user_id, срок не истек
        QA-->>BA: ok или ErrAdmissionInvalid
    end
```

### 5.2 Hold места: два конкурента на одно место

Здесь живет инвариант I1. Гарантию дает partial unique index `uniq_active_hold (event_id, seat_id) WHERE status = 'active'`, advisory-lock только выстраивает конкурентов в очередь до вставки.

```mermaid
sequenceDiagram
    autonumber
    actor A as Пользователь A
    actor B as Пользователь B
    participant H as booking/http
    participant S as booking/app.CreateHold
    participant C as catalog.api
    participant Q as queue.api
    participant PG as PostgreSQL

    par оба целятся в seat 17
        A->>H: POST /v1/holds + X-Admission-Token
        B->>H: POST /v1/holds + X-Admission-Token
    end
    H->>S: CreateHold(event, seat 17, user)
    S->>C: GetEvent: sales_state = open?
    S->>C: GetSeat: место на площадке этого мероприятия?
    S->>Q: Validate(token, event, user)
    S->>PG: BEGIN
    S->>PG: SELECT pg_advisory_xact_lock(hashtext(event), seat)
    Note over S,PG: запрос B ждет здесь до COMMIT или ROLLBACK транзакции A
    S->>PG: SELECT count(*) FROM holds WHERE event, user AND status IN (active, confirmed)
    S->>S: domain.CanHoldMore(n): лимит 4, FR-7
    S->>PG: SELECT EXISTS confirmed hold на (event, seat): место уже продано?
    S->>PG: INSERT holds(status = active, expires_at = now() + HOLD_TTL)
    S->>PG: COMMIT
    S-->>A: 201 {hold, expires_at}

    Note over S,PG: lock освобожден, транзакция B продолжается
    S->>PG: BEGIN, lock, INSERT holds(event, seat 17, B)
    PG-->>S: unique_violation uniq_active_hold
    S->>PG: ROLLBACK
    S-->>B: 409 seat_held
```

Коды ответа этого сценария: 201 создан, 409 `seat_held` или `seat_sold` или `sales_closed`, 422 `hold_limit` или `seat_not_in_event`, 401 `admission_required`, 503 `db_busy` при исчерпании пула, 503 `timeout` при исчерпании бюджета хендлера.

### 5.3 Заказ: идемпотентность по ключу

```mermaid
sequenceDiagram
    autonumber
    actor U as Покупатель
    participant H as booking/http
    participant S as booking/app.CreateOrder
    participant PG as PostgreSQL

    U->>H: POST /v1/orders, Idempotency-Key: k1, {hold_ids, user_id}
    H->>S: CreateOrder(in)
    S->>PG: SELECT orders WHERE idempotency_key = k1
    alt заказ по ключу уже есть
        S->>S: тот же user и тот же набор hold_ids?
        alt совпадает
            S-->>U: 200 {order}, повтор возвращает тот же результат
        else не совпадает
            S-->>U: 409 idempotency_conflict
        end
    else заказа нет
        S->>PG: BEGIN
        S->>PG: SELECT holds WHERE id = ANY($1) ORDER BY id FOR UPDATE
        S->>S: domain.NewOrder: все hold активны, не истекли, один пользователь, одно мероприятие
        S->>PG: SELECT EXISTS hold в открытом заказе?
        S->>PG: INSERT orders(status = pending, amount, idempotency_key), INSERT order_items
        S->>PG: COMMIT
        S-->>U: 201 {order pending}
    end
```

Сортировка `ORDER BY id` при блокировке строк убирает взаимные блокировки двух заказов с пересекающимися hold. Гонку двух одинаковых запросов ловит `orders.idempotency_key UNIQUE`: проигравший перечитывает заказ победителя и отдает 200.

### 5.4 Оплата: списание, подтверждение, билет, уведомление

Весь путь синхронный. Четыре шага в четырех транзакциях, между ними внешний вызов PSP.

```mermaid
sequenceDiagram
    autonumber
    actor U as Покупатель
    participant S as booking/app.PayOrder
    participant PG as PostgreSQL
    participant P as payment (порт PaymentGateway)
    participant PSP as Эмулятор PSP
    participant T as ticketing (порт TicketIssuer)
    participant N as notification

    U->>S: POST /v1/orders/{id}/pay
    S->>PG: SELECT order
    alt статус paid
        S->>T: Issue(...), идемпотентно
        S-->>U: 200 {order paid}
    else статус failed или expired
        S-->>U: 409 order_state
    end

    rect rgba(0,0,0,0.04)
    Note over S,PG: транзакция 1: hold еще живы?
    S->>PG: SELECT order FOR UPDATE, SELECT holds FOR UPDATE
    alt хотя бы один hold истек
        S->>PG: UPDATE orders SET status = expired
        S-->>U: 409 order_expired
    end
    end

    S->>P: Charge(order_id, amount, idempotency_key)
    P->>PG: SELECT payments WHERE idempotency_key
    alt платеж по ключу уже есть
        P-->>S: сохраненный результат, PSP не вызывается
    else нет
        P->>PSP: charge, задержка PSP_DELAY_MS, отказ с вероятностью PSP_FAIL_RATE
        PSP-->>P: succeeded + psp_ref или declined
        P->>PG: INSERT payments(status, psp_ref, idempotency_key)
    end

    alt PSP отказал
        rect rgba(0,0,0,0.04)
        Note over S,PG: транзакция 2: откат, место возвращается в продажу
        S->>PG: UPDATE orders SET status = failed
        S->>PG: UPDATE holds SET status = released
        end
        S-->>U: 402 payment_failed
    else PSP подтвердил
        rect rgba(0,0,0,0.04)
        Note over S,PG: транзакция 3: подтверждение
        S->>PG: UPDATE holds SET status = confirmed
        S->>PG: UPDATE orders SET status = paid
        end
        S->>S: fault-flag FAULT_CRASH_AFTER_COMMIT=order.paid, точка разрыва для занятия 3
        S->>T: Issue(order, event, user, seats)
        loop по каждому месту заказа
            T->>PG: INSERT tickets ON CONFLICT (order_id, seat_id) DO NOTHING
        end
        alt выпущен хотя бы один новый билет
            T->>N: Send(user, tickets_issued, codes)
            N->>N: доставка в структурный лог
            N->>PG: INSERT notifications(status = sent или failed)
        end
        S-->>U: 200 {order paid}
    end
```

Два известных разрыва этой цепочки, оба намеренные:

- падение процесса между транзакцией 3 и выпуском билета оставляет оплаченный заказ без билета, то есть нарушает I3. Повторный `pay` чинит ситуацию, `Issue` идемпотентен по `(order_id, seat_id)`. На занятии 3 шаги выпуска и уведомления уезжают в outbox.
- если hold нельзя подтвердить после успешного списания, деньги списаны и место потеряно. Сейчас это пишется в лог как требующее компенсации, заказ помечается `failed`. Компенсация появляется на занятии 7 вместе с сагой.

### 5.5 Истечение hold

```mermaid
sequenceDiagram
    autonumber
    participant E as booking/app.Expirer, тикер 5 с
    participant PG as PostgreSQL
    participant M as metrics

    loop каждый тик, контекст ограничен длиной интервала
        loop батчами по HOLD_EXPIRER_BATCH = 500
            E->>PG: WITH batch AS (SELECT id FROM holds WHERE active AND expires_at <= now() ORDER BY expires_at LIMIT 500 FOR UPDATE SKIP LOCKED) UPDATE holds SET status = released
            PG-->>E: список освобожденных мест
            E->>PG: batch INSERT free-событий в outbox в той же транзакции
            E->>M: expirer_batch_size.Observe(n)
            E->>E: пауза 50 мс между батчами
        end
        E->>PG: UPDATE orders SET status = expired для заказов, чьи hold ушли
        E->>PG: SELECT count(*) FROM holds WHERE status = active
        E->>M: holds_active.Set(n)
    end
```

`SKIP LOCKED` и батчи выбраны по результату замера: один большой `UPDATE` под штормом давал всплеск p99 ровно в момент замера. Параллельные `pay` и `release` при этом не блокируются.

### 5.6 Карта зала

Текущая проекция `catalog_seat_state` наполняется через transactional outbox: booking записывает `SeatStateChanged` в транзакции hold/release/pay/expire. Отдельный polling-projector читает события `aggregatetype=seat` по монотонному `seq`, применяет их одним set-based batch и инвалидирует каждый сектор один раз. Дедупликация ID и high-watermark `(event, seat)` делают повторную доставку идемпотентной; ack выполняется только после Apply и инвалидации кэша.

Самый тяжелый запрос baseline: 40 000 мест сериализуются целиком на каждый вызов, около 1.8 МБ ответа.

```mermaid
sequenceDiagram
    autonumber
    actor U as Покупатель
    participant CH as catalog/http
    participant C as catalog/app.SeatMap
    participant PG as PostgreSQL
    participant B as booking.api.SeatStates

    U->>CH: GET /v1/events/{id}/seatmap
    CH->>C: SeatMap(eventID)
    C->>PG: SELECT event
    C->>PG: SELECT sectors WHERE venue
    C->>PG: SELECT seats WHERE venue, 40 000 строк
    C->>B: SeatStates(eventID)
    B->>PG: SELECT seat_id, status FROM holds WHERE event AND status IN (active, confirmed)
    Note over B,PG: JOIN между модулями запрещен правилом зависимостей, поэтому два запроса и склейка в памяти
    B-->>C: map[seat_id]SeatState
    C->>C: BuildSeatMap: секторы, места, статусы free, held, sold
    C-->>U: 200 JSON целиком
```

### 5.7 Билеты пользователя

Чтение через границу модулей: `ticketing` не владеет заказами и берет их через `booking.api`.

```mermaid
sequenceDiagram
    autonumber
    actor U as Покупатель
    participant TH as ticketing/http
    participant B as booking.api.ListUserOrders
    participant T as ticketing/app.ListByOrders
    participant PG as PostgreSQL

    U->>TH: GET /v1/users/{id}/tickets
    TH->>B: ListUserOrders(userID)
    B->>PG: SELECT orders + order_items пользователя
    TH->>TH: оставить только заказы в статусе paid
    TH->>T: ListByOrders(ids)
    T->>PG: SELECT tickets WHERE order_id = ANY($1)
    T-->>U: 200 {tickets: [{code, seat_id, issued_at}]}
```

### 5.8 Запуск и остановка процесса

```mermaid
sequenceDiagram
    autonumber
    participant M as main.run
    participant PG as PostgreSQL
    participant VK as Valkey
    participant HTTP as http.Server
    participant EXP as Expirer

    M->>M: config.FromEnv, faults.FromEnv, logger, metrics
    M->>PG: Migrate по DATABASE_URL_DIRECT, если MIGRATE_ON_START
    M->>PG: Seed 3 мероприятия по SEED_SEATS_PER_EVENT мест, если SEED_ON_START
    M->>PG: NewPool: MaxConns, MinConns, AcquireTimeout, ping с ретраями
    M->>VK: cache.New, ping
    M->>M: собрать модули по правилу зависимостей
    M->>EXP: запустить горутину expirer
    M->>HTTP: ListenAndServe
    Note over HTTP: /readyz отдает 200, когда отвечают PostgreSQL и Valkey, и печатает диагностику: gomaxprocs, gomemlimit, db_max_conns, pgbouncer

    Note over M,EXP: SIGTERM
    M->>HTTP: Shutdown с таймаутом SHUTDOWN_TIMEOUT, текущие запросы дорабатывают
    M->>EXP: отмена контекста после остановки приема
    M->>PG: pool.Close
    M->>VK: close
```

Порядок остановки выбран так, чтобы expirer не выключился раньше, чем дорабатывают запросы: сначала перестаем принимать новое, затем гасим фон.

## 6. Данные и инварианты

```mermaid
erDiagram
    venues ||--o{ sectors : ""
    sectors ||--o{ seats : ""
    venues ||--o{ events : ""
    events ||--o{ holds : ""
    seats ||--o{ holds : ""
    events ||--o{ orders : ""
    orders ||--o{ order_items : ""
    holds ||--|| order_items : ""
    orders ||--o{ payments : ""
    orders ||--o{ tickets : ""
```

| Инвариант | Формулировка | Чем обеспечен |
|---|---|---|
| I1 | на `(event, seat)` не более одного активного hold | `uniq_active_hold (event_id, seat_id) WHERE status = 'active'` плюс `pg_advisory_xact_lock` до вставки |
| I2 | на `(event, seat)` не более одного билета | проверка `SeatSold` в транзакции hold, `tickets_order_seat_uniq`, `tickets.code UNIQUE` |
| I3 | у каждого `paid` заказа есть билет на каждую позицию | синхронный вызов `Issue` внутри `pay`, идемпотентный повтор при повторном `pay` |
| I4 | не более 4 билетов на пользователя на мероприятие | `CountUserHolds` и `domain.CanHoldMore` внутри транзакции hold |

Идемпотентность держится на уникальных ключах: `orders.idempotency_key`, `payments.idempotency_key`, `tickets (order_id, seat_id)`. Проверяет все это `tools/invariant-checker` (`make invariants`) по данным PostgreSQL после каждого прогона нагрузки.

## 7. Бюджет запроса и таймауты

| Уровень | Параметр | Значение | Что происходит при исчерпании |
|---|---|---|---|
| HTTP-сервер | `ReadHeaderTimeout` / `ReadTimeout` / `WriteTimeout` / `IdleTimeout` | 5 с / 10 с / 15 с / 60 с | соединение закрывается |
| Хендлер | `HANDLER_TIMEOUT` | 3 с | 503 `timeout` |
| Пул pgx | `DB_ACQUIRE_TIMEOUT` | 2 с | 503 `db_busy` плюс `Retry-After`, счетчик `db_pool_acquire_timeouts_total` |
| Пул pgx | `DB_MAX_CONNS` / `DB_MIN_CONNS` | 200 на baseline, 40 с шага A / 5 | очередь за соединением живет в приложении |
| PostgreSQL | `statement_timeout`, `idle_in_transaction_session_timeout` | 5 с | запрос снимается сервером БД |
| PgBouncer | `default_pool_size`, `query_wait_timeout` | 20, 5 с | клиент ждет серверное соединение |
| Домен | `HOLD_TTL`, `ADMISSION_TTL` | 10 мин, 15 мин | hold освобождается expirer, токен истекает |
| Процесс | `SHUTDOWN_TIMEOUT` | 10 с | принудительная остановка сервера |

Каждый уровень ниже быстрее уровня выше: хендлер сдается раньше, чем сервер оборвет запись, а ожидание соединения короче бюджета хендлера. Это дает быстрый отказ вместо зависшего клиента.

## 8. Наблюдаемость

- RED-метрики по маршрутам: `http_requests_total`, `http_request_duration_seconds` с меткой шаблона маршрута chi.
- База: `db_query_duration_seconds` по имени запроса (имя берется из комментария `/* booking.lock_seat */` в начале SQL), `db_pool_acquire_wait_seconds`, `db_pool_acquire_timeouts_total`.
- Домен: `holds_active`, `expirer_batch_size`, `hold_contention_total`.
- Логи структурные (slog), в каждом запросе `request_id` от chi.
- Профили: `/debug/pprof` с продленным дедлайном записи, Alloy тянет CPU, heap и goroutine каждые 15 с в Pyroscope.
- Готовность: `/readyz` проверяет PostgreSQL и Valkey и печатает `gomaxprocs`, `gomemlimit`, `db_max_conns`, версию PgBouncer.

Метрика `cache_ops_total` наполняется при работе версионированного кэша карты. Метрики `eventbus_*` остаются для исторической in-process реализации; текущий путь SeatStateChanged использует durable outbox.

## 9. Что baseline делает плохо и куда это чинится

| Слабое место baseline | Проявление под штормом | Где чинится |
|---|---|---|
| карта зала целиком на каждый запрос | `catalog.seats_by_venue` 102 мс на вызов, 68 % CPU приложения на сборке ответа | шаг B: карта по секторам, проекция, версионированный кэш в Valkey, ETag |
| нет индексов под лимит 4, expirer и статусы мест | Seq Scan по `holds`, `count_user_holds` 3 мс на вызов | шаг A: `CREATE INDEX CONCURRENTLY` в миграциях 0006-0010 |
| пул 200 соединений при `max_connections = 100` | 503 `db_busy`, в прошлых прогонах `too many clients` | шаг A: пул 40, PgBouncer в transaction pooling |
| advisory-lock на горячем ряду | сотни сессий с `wait_event = advisory`, p99 hold 1.7 с | шаг C: `INSERT ... ON CONFLICT DO NOTHING RETURNING` по partial unique index, плюс `POST /v1/holds/any` |
| допуск выдается сразу, очереди нет | весь трафик идет в консистентную часть | шаг D: настоящая очередь и admission rate |
| билет и уведомление выпускаются внутри `pay` | падение после commit теряет билет, I3 нарушается | занятие 3: outbox, CDC, отдельные потребители |
| нет аутентификации и rate limit | боты неотличимы от людей | занятие 6 |

Полные цифры baseline: `lessons/02/baseline.md`. Решение о стартовой архитектуре и список осознанных упрощений: `docs/adr/ADR-001-modular-monolith.md`.
