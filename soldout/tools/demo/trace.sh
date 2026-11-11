#!/usr/bin/env bash
# trace.sh last | slow [TraceQL] | json TRACE_ID — трейсы из Tempo без Grafana (занятие 4).
#   last  — trace_id последнего pay из логов soldout и ссылка на Grafana Explore
#   slow  — TraceQL через Tempo API за последние 5 минут (по умолчанию pay дольше 1 с)
#   json  — трейс целиком в k6/results/trace-<id>.json (фолбэк для агента, если Tempo MCP не подключился)
set -euo pipefail
TEMPO=${TEMPO_URL:-http://localhost:3200}
GRAFANA=${GRAFANA_URL:-http://localhost:3000}
COMPOSE=${COMPOSE:-docker compose -f docker-compose.yml -f compose/lesson-02.yml -f compose/lesson-04.yml}

explore_link() { # explore_link TRACEQL_OR_ID
  python3 - "$GRAFANA" "$1" <<'PY'
import json, sys, urllib.parse
grafana, q = sys.argv[1], sys.argv[2]
panes = {"a": {"datasource": "tempo", "queries": [{"refId": "A", "datasource": {"type": "tempo", "uid": "tempo"},
         "queryType": "traceql", "query": q}], "range": {"from": "now-1h", "to": "now"}}}
print(f"{grafana}/explore?schemaVersion=1&orgId=1&panes=" + urllib.parse.quote(json.dumps(panes, separators=(',', ':'))))
PY
}

case "${1:-}" in
last)
  id=$($COMPOSE logs --no-log-prefix soldout 2>/dev/null | grep 'route=/v1/orders/{id}/pay\|"route":"/v1/orders/{id}/pay"' | tail -1 |
       sed -nE 's/.*trace_id[=":]+([0-9a-f]{32}).*/\1/p')
  [ -n "$id" ] || { echo "в логах soldout нет pay с trace_id: сначала make pay-demo"; exit 1; }
  echo "trace_id: $id"
  echo "Grafana:  $(explore_link "$id")"
  echo "JSON:     $TEMPO/api/v2/traces/$id"
  ;;
slow)
  q=${2:-'{ name = "POST /v1/orders/{id}/pay" && duration > 1s }'}
  end=$(date +%s); start=$((end - 300))
  echo "TraceQL: $q  (последние 5 минут)"
  curl -sG "$TEMPO/api/search" --data-urlencode "q=$q" --data-urlencode "start=$start" --data-urlencode "end=$end" \
       --data-urlencode "limit=${LIMIT:-20}" |
  python3 <(cat <<'PY'
import json, sys
d = json.load(sys.stdin)
tr = d.get("traces", [])
print(f"найдено: {len(tr)}")
for t in sorted(tr, key=lambda t: -t.get("durationMs", 0)):
    print(f'{t["traceID"].rjust(32, "0")}  {t.get("durationMs", 0):>6} мс  {t.get("rootServiceName", "")}  {t.get("rootTraceName", "")}')
PY
)
  echo "Grafana:  $(explore_link "$q")"
  ;;
json)
  id=${2:?укажите TRACE=<trace_id>}
  mkdir -p k6/results
  curl -sf "$TEMPO/api/v2/traces/$id" -o "k6/results/trace-$id.json"
  echo "k6/results/trace-$id.json"
  ;;
*)
  echo "использование: $0 last | slow [TraceQL] | json TRACE_ID"; exit 2 ;;
esac
