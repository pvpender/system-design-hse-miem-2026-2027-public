# Занятие 4. Демо: все команды по актам

Все команды из `soldout/`. Стенд занятия 4 по умолчанию (`LESSON=04`): оверлеи `lesson-02` + `lesson-04`, профили `obs` и `cdc`. Цифры ожиданий: прогоны 26.09.2026 (`results.md`).

## Подготовка (до пары, 15 минут)

```bash
tools/doctor.sh
docker info --format '{{.MemTotal}}'          # нужно ≥ 8 ГБ у Docker Desktop
git switch demo/l4-problem && make down && make up
make pay-demo                                  # pay около 1 с
make grafana-token                             # токен для mcp-grafana в .env.local (make up делает сам)
```

Вкладки: Grafana `soldout-red`, `soldout-traces`, `soldout-events`, Explore → Tempo; kafka-ui http://localhost:8088 (появится на cdc). Терминалы: два в `soldout/`, третий с `claude` в `soldout/` (`/mcp`: `postgres`, `tempo`, `grafana` connected; `tempo` заработает с ветки otel).

Фолбэк на каждый акт: `lessons/04/act{1,2,3}/README.md` и скриншоты там же.

## Акт 1. Проблема (ветка `demo/l4-problem`, 8 минут)

```bash
make pay-demo            # join/hold/order по 2-10 мс, pay около 1 с
make checkout-storm      # 90 с checkout (150 VU) + фон 400 итераций/с на мероприятии 1
```

Пока идет (терминал 2), инструменты занятия 3 по очереди:

```bash
make docker-stats        # soldout 40-50 % одного ядра из 2, postgres 50-65 %
make pprof-top           # syscall/runtime, бизнес-функций в топе нет
make pg-top              # все запросы < 0.3 мс в среднем, за прогон ~19 с работы БД
```

Итог k6: pay p99 около 2.3 с (порог 800 мс красный), ошибок 0 %. Вопрос студентам: где 1.25 с средней оплаты? RED показывает только итог; разбивки по этапам нет.

Добавить после k6: `make psql` → `SELECT status, count(*) FROM notifications GROUP BY 1;` около 30 писем `failed`, а клиент по ним получил 200.

## Акт 2. Трейсинг (ветка `demo/l4-otel`, 16 минут)

```bash
make switch BRANCH=demo/l4-otel     # +otel-collector, +Tempo; данные сохраняются
make pay-demo
make trace-last                     # trace_id и ссылка на Grafana Explore
```

Открыть ссылку: водопад `POST /v1/orders/{id}/pay` → `booking.PayOrder` → `payment.Charge` → `psp.emulate` 300 мс → `ticketing.Issue` → `notification.Send` → `mailgw.POST /send` ~0.9 с и серверный `POST /send` сервиса mailgw рядом. SQL-span'ы по миллисекунде.

Трейс с повтором к шлюзу:

```bash
make checkout-storm                 # можно не ждать конца: трейсы появляются сразу
make traces-slow Q='{ name = "mailgw.POST /send" && span.retry.attempt = 1 }'
```

Показать: 502 красным, повтор отдельным span'ом, `pgx.acquire` в начале (очередь на пул под фоном). Затем дашборд `soldout-traces`: то же самое агрегированно.

Агент (терминал 3), промпт 1 из `ai-session.md`: "Найди в Tempo самые медленные pay за 10 минут и объясни по одному трейсу, где время". Ждем вызовы `traceql-search` → `get-trace`.

## Акт 3. Не в пути запроса (ветка `demo/l4-otel`, 10 минут)

Вопрос: как убрать почту из запроса? Ответ зала обычно "горутиной". Проверяем:

```bash
make invariants          # сначала базовая линия: I5 бывает красным уже здесь
make break-goroutine     # FAULT_NOTIFY_MODE=goroutine FAULT_CRASH_AFTER_COMMIT=order.paid, pay-demo, invariants
```

pay 200 за ~320 мс, билет есть, процесс упал через 100 мс, письма нет: **I5 красный** (первый красный инвариант курса). Это dual write. Правильно: событие в outbox в той же транзакции.

Если I5 был красным еще до слома: это письма, потерянные в синхронном режиме. Pay упирался в бюджет 3 с, контекст отменялся, и запись уведомления не выполнялась, а клиент получал 200. В прогоне 26.09 таких заказов было 42 на ~21 000 оплат. Это та же потеря, только тихая; на ветке cdc их дошлет `make repair-i5`.

```bash
make restore
```

## Акт 4. Outbox, Kafka, Debezium, notifier (ветка `demo/l4-cdc`, 22 минуты)

```bash
make switch BRANCH=demo/l4-cdc      # +kafka, +connect, +kafka-ui, +notifier; коннектор регистрируется сам
make cdc-status                     # слот active, outbox_rows = 0, connector RUNNING
make pay-demo                       # pay около 330 мс
make trace-last
```

По порядку:

0. `make repair-i5` → оплаченные заказы без письма (след акта 3) получают OrderPaid в outbox, notifier досылает письма, I5 зеленый. Починка стала возможной, потому что у доставки появился один вход.
1. `make psql` → `SELECT count(*) FROM outbox;` = 0, а события идут: строка вставляется и удаляется в одной транзакции, Debezium читает INSERT из WAL.
2. kafka-ui → Topics → `outbox.event.order` → Messages: ключ `order_id`, заголовки `id`, `type`, `traceparent`.
3. `docker compose logs notifier | tail` → `trace_id` тот же, что у pay.
4. Трейс из `make trace-last`: `booking.PayOrder` с событием `outbox.append`, затем `notifier.consume` (сервис notifier) → `mailgw.POST /send`. Разрыв между ними ~0.3-1 с: это CDC и опрос потребителя.

```bash
make checkout-storm                 # pay p99 падает до PSP + confirm
```

Сломы:

```bash
make kill-notifier   # checkout 90 с; notifier стоп на 20-й секунде, старт на 60-й; дашборд soldout-events: лаг растет и уходит в 0
                     # в конце: paid = notifications, duplicates = 0, not_sent = 0
make poison          # невалидное сообщение: 5 попыток за ~8 с, потом DLQ, поток идет
make dlq             # заголовки error, attempts, original_offset
make invariants      # I1-I6 зеленые, включая I5
```

Если время есть: падение после commit теперь письмо не теряет.

```bash
FAULT_CRASH_AFTER_COMMIT=order.paid make restore && make pay-demo; make restore && make invariants
```

Вариант A ADR-003 одной переменной (без Debezium): `make up-poll`, `make pay-demo`, `make restore`.

## ADR-003 (8 минут)

`docs/adr/ADR-003-event-delivery-and-notifier.md`: таблица A/B/C, голосование, known limitations. Вслух про слот репликации: остановить Connect (`docker compose stop connect`), сделать `make checkout`, `make cdc-status` → `wal_behind` растет. Никто не читает слот, WAL копится до заполнения диска.

## ИИ-нить (7 минут)

Промпты 1 и 2 из `ai-session.md`. Второй: "Сравни p99 pay до и после перехода на outbox по Prometheus и объясни разницу по трейсам" (Grafana MCP `query_prometheus` + Tempo MCP).

## Итоги и HW2 (5 минут)

`hw/hw2/HW_2.md`, срок занятие 6. Тизер занятия 5: notifier масштабируется отдельно от монолита, `notifier_lag` как сигнал HPA.

## После пары

```bash
make down
```
