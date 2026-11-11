#!/usr/bin/env bash
# Проверка окружения студента для курса «Системный дизайн: практика».
# Использование: tools/doctor.sh
set -u

ok()   { printf '  [ OK ] %s\n' "$*"; }
warn() { printf '  [WARN] %s\n' "$*"; WARNINGS=$((WARNINGS+1)); }
fail() { printf '  [FAIL] %s\n' "$*"; FAILURES=$((FAILURES+1)); }
WARNINGS=0; FAILURES=0

echo "== Docker"
if command -v docker >/dev/null 2>&1; then
  ok "docker: $(docker --version 2>/dev/null)"
  if docker info >/dev/null 2>&1; then
    ok "docker daemon доступен"
    MEM=$(docker info --format '{{.MemTotal}}' 2>/dev/null || echo 0)
    MEM_GB=$(( MEM / 1024 / 1024 / 1024 ))
    if [ "$MEM_GB" -ge 8 ]; then ok "память Docker: ${MEM_GB} ГБ"
    elif [ "$MEM_GB" -ge 6 ]; then warn "память Docker: ${MEM_GB} ГБ — хватит для занятий 1–2 и lite-профиля"
    else fail "память Docker: ${MEM_GB} ГБ — поднимите лимит до 8 ГБ (Settings → Resources)"; fi
    ok "CPU: $(docker info --format '{{.NCPU}}' 2>/dev/null)"
  else
    fail "docker daemon не запущен (запустите Docker Desktop / OrbStack)"
  fi
else
  fail "docker не найден: https://docs.docker.com/get-docker/"
fi

echo "== Docker Compose"
if docker compose version >/dev/null 2>&1; then
  ok "$(docker compose version 2>/dev/null)"
else
  fail "docker compose (v2) не найден"
fi

echo "== Реестры образов"
check_registry() {
  local name="$1" url="$2"
  if curl -fsS --max-time 8 -o /dev/null "$url" 2>/dev/null; then ok "$name доступен"
  else warn "$name недоступен ($url) — см. infra/docker-mirror.md"; fi
}
check_registry "Docker Hub"        "https://registry-1.docker.io/v2/"
check_registry "mirror.gcr.io"     "https://mirror.gcr.io/v2/"
check_registry "dockerhub.timeweb.cloud" "https://dockerhub.timeweb.cloud/v2/"
check_registry "cr.yandex/mirror"  "https://cr.yandex/v2/"

echo "== hello-world"
if docker info >/dev/null 2>&1; then
  if docker run --rm hello-world >/dev/null 2>&1; then ok "docker run hello-world"
  else warn "docker run hello-world не удался — проверьте зеркало/сеть"; fi
fi

echo "== Инструменты"
command -v make >/dev/null 2>&1 && ok "make: $(make --version 2>/dev/null | head -1)" || fail "make не найден"
command -v git  >/dev/null 2>&1 && ok "git: $(git --version)" || fail "git не найден"
command -v go   >/dev/null 2>&1 && ok "go (опционально): $(go version)" || warn "go не найден — не обязателен, сборка идёт в Docker"
command -v k6   >/dev/null 2>&1 && ok "k6 (опционально): $(k6 version 2>/dev/null | head -1)" || warn "k6 не найден — не обязателен, стрельба идёт в контейнере grafana/k6"

echo "== Итог"
if [ "$FAILURES" -eq 0 ]; then
  echo "  Окружение готово (предупреждений: $WARNINGS)."
  exit 0
else
  echo "  Ошибок: $FAILURES, предупреждений: $WARNINGS. Исправьте [FAIL] и запустите снова."
  exit 1
fi
