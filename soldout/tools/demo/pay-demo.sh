#!/usr/bin/env bash
# pay-demo: один проход покупки curl'ом — join → hold (место SEAT, по умолчанию 777 мероприятия 2) → order → pay.
# Печатает время каждого вызова (curl -w). Если место уже продано прошлым прогоном, берёт следующее свободное.
set -euo pipefail
BASE=${BASE_URL:-http://localhost:8080}
EVENT_NO=${EVENT_NO:-2}
EVENT=10000000-0000-4000-8000-$(printf '%012x' "$EVENT_NO")
# мест на мероприятие: из сводки карты зала (по 1000 в секторе), чтобы одинаково работать на полном стенде и в lite
SEATS=$(curl -s "$BASE/v1/events/$EVENT/seatmap" | python3 -c 'import json,sys; print(sum(s["total"] for s in json.load(sys.stdin)["sectors"]))' 2>/dev/null || true)
SEATS=${SEATS:-${SEED_SEATS_PER_EVENT:-40000}}
SEAT=$(( (EVENT_NO - 1) * SEATS + ${SEAT:-777} ))
USER_ID=$(python3 -c 'import uuid; print(uuid.uuid4())')
TMP=$(mktemp); trap 'rm -f "$TMP"' EXIT
json() { python3 -c "import json,sys; print(json.load(open('$TMP'))$1)"; }

call() { # call NAME METHOD URL [BODY] [HEADERS...] → печатает статус и время, тело в $TMP
  local name=$1 method=$2 url=$3 body=${4:-}; shift 4 || shift $#
  local args=(-s -o "$TMP" -w '%{http_code} %{time_total}\n' -X "$method" -H 'Content-Type: application/json')
  for h in "$@"; do args+=(-H "$h"); done
  [ -n "$body" ] && args+=(-d "$body")
  read -r code secs < <(curl "${args[@]}" "$BASE$url")
  printf '%-7s %s  %6.0f мс\n' "$name" "$code" "$(python3 -c "print($secs*1000)")"
  echo "$code" > "$TMP.code"
}

call join POST "/v1/events/$EVENT/queue/join" "{\"user_id\":\"$USER_ID\"}"
TOKEN=$(json "['token']")

for try in $(seq 1 50); do
  call hold POST /v1/holds "{\"event_id\":\"$EVENT\",\"seat_id\":$SEAT,\"user_id\":\"$USER_ID\"}" "X-Admission-Token: $TOKEN"
  [ "$(cat "$TMP.code")" = 201 ] && break
  SEAT=$((SEAT + 1))
done
HOLD=$(json "['id']")

call order POST /v1/orders "{\"hold_ids\":[\"$HOLD\"],\"user_id\":\"$USER_ID\"}" "Idempotency-Key: $(python3 -c 'import uuid; print(uuid.uuid4())')"
ORDER=$(json "['id']")

call pay POST "/v1/orders/$ORDER/pay" "" "Idempotency-Key: pay-$ORDER"
echo "pay:    $(python3 -c "import json; d=json.load(open('$TMP')); print(d.get('status') or d)")"
rm -f "$TMP.code"
echo "user_id=$USER_ID order_id=$ORDER seat_id=$SEAT"
