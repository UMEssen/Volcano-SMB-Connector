#!/usr/bin/env bash
# Integration test: Samba + Kafka in Docker, the watcher runs on the host.
#
#   tests/integration/run.sh            # build, start env, run scenarios, tear down
#   KEEP_ENV=1 tests/integration/run.sh # leave containers running afterwards
#
# Scenarios: baseline (no events for pre-existing files), new file, file being
# written, service restart, Samba restart, many files at once (overflow path),
# polling-only fallback (USE_WATCHER=false).
set -euo pipefail

HERE="$(cd "$(dirname "$0")" && pwd)"
ROOT="$(cd "$HERE/../.." && pwd)"
COMPOSE=(docker compose -f "$HERE/docker-compose.yml")
export SMB_PORT="${SMB_PORT:-14450}" KAFKA_PORT="${KAFKA_PORT:-19092}"
HTTP_PORT="${HTTP_PORT:-18080}"
WORK="$(mktemp -d "${TMPDIR:-/tmp}/smbw-it.XXXXXX")"
LOG="$WORK/watcher.log"
BIN="$ROOT/target/debug/smb-kafka-watcher"
MANY="${MANY_FILES:-3000}"
PID=""
FAILS=0

cleanup() {
  [ -n "$PID" ] && kill "$PID" 2>/dev/null || true
  if [ -z "${KEEP_ENV:-}" ]; then "${COMPOSE[@]}" down -v --remove-orphans >/dev/null 2>&1 || true; fi
  echo "logs: $LOG"
}
trap cleanup EXIT

say()  { printf '\n=== %s\n' "$*"; }
ok()   { printf '  ok   %s\n' "$*"; }
bad()  { printf '  FAIL %s\n' "$*"; FAILS=$((FAILS+1)); }
smb()  { "${COMPOSE[@]}" exec -T samba sh -c "$1"; }

# start_watcher [VAR=value ...]: extra env overrides the defaults
start_watcher() {
  env SMB_SERVER="127.0.0.1:$SMB_PORT" SMB_SHARE=data SMB_USER=testuser SMB_PASSWORD=testpass \
  KAFKA_BOOTSTRAP_SERVERS="127.0.0.1:$KAFKA_PORT" KAFKA_CREATE_TOPICS=true \
  STABILITY_DELAY_SECS=3 RECONCILE_INTERVAL_SECS=30 HTTP_PORT="$HTTP_PORT" BACKOFF_MAX_SECS=4 \
  RUST_LOG="${RUST_LOG:-info}" NO_COLOR=1 "$@" "$BIN" >>"$LOG" 2>&1 &
  PID=$!
  for _ in $(seq 1 60); do
    curl -sf "localhost:$HTTP_PORT/readyz" >/dev/null && return 0
    sleep 1
  done
  echo "watcher did not become ready"; tail -30 "$LOG"; exit 1
}

stop_watcher() {
  kill -TERM "$PID"; wait "$PID" || true; PID=""
}

# All events as "key<TAB>value" lines. Reads the (single) partition directly,
# without a consumer group: a group join can outlast the idle timeout and
# return nothing.
events() {
  "${COMPOSE[@]}" exec -T kafka /opt/kafka/bin/kafka-console-consumer.sh \
    --bootstrap-server kafka:9092 --topic smb-watcher.events --partition 0 --offset earliest \
    --property print.key=true --timeout-ms 3000 2>/dev/null || true
}
count_key() { events | awk -F'\t' -v k="$1" '$1==k' | wc -l | tr -d ' '; }

# wait_for_key KEY [TIMEOUT]: wait until at least one event exists for KEY
wait_for_key() {
  local k="$1" t="${2:-30}" end=$((SECONDS + ${2:-30}))
  while [ $SECONDS -lt $end ]; do
    [ "$(count_key "$k")" -ge 1 ] && return 0
    sleep 1
  done
  return 1
}

expect_once() {
  local k="$1" n; n="$(count_key "$k")"
  [ "$n" = 1 ] && ok "$k: exactly one event" || bad "$k: $n events (want 1)"
}

say "build"
(cd "$ROOT" && cargo build --quiet)

say "start Samba + Kafka"
"${COMPOSE[@]}" down -v --remove-orphans >/dev/null 2>&1 || true
"${COMPOSE[@]}" up -d --build --wait >/dev/null
smb 'mkdir -p /shares/data/old && echo old > /shares/data/old/preexisting.txt && echo x > /shares/data/root.txt'

say "1. baseline: first start, existing files produce no events"
start_watcher
sleep 5
n="$(events | wc -l | tr -d ' ')"
[ "$n" = 0 ] && ok "no events for pre-existing files" || bad "$n events after baseline"

say "2. new file"
smb 'mkdir -p /shares/data/in/sub && echo hello > /shares/data/in/sub/new.pdf'
wait_for_key in/sub/new.pdf 30 && expect_once in/sub/new.pdf || bad "no event for in/sub/new.pdf"
events | grep -q '"source":"watcher"' && ok "detected via watcher" || bad "event not from watcher"
smb 'echo tmp > /shares/data/in/ignored.tmp; echo tmp > /shares/data/in/~\$lock.docx'

say "3. large file being written (must emit once, with final size)"
smb 'for i in 1 2 3 4 5 6 7 8; do dd if=/dev/zero bs=1M count=4 2>/dev/null >> /shares/data/in/big.bin; sleep 1; done'
wait_for_key in/big.bin 30 && expect_once in/big.bin || bad "no event for in/big.bin"
events | grep -q '"path":"in/big.bin","size":33554432' && ok "big.bin size 32 MiB" || bad "big.bin event has wrong size: $(events | grep big.bin)"
n="$(events | grep -c -e 'ignored.tmp' -e 'lock.docx' || true)"
[ "$n" = 0 ] && ok "ignore patterns honoured" || bad "$n events for ignored files"

say "4. service restart (file created while down)"
stop_watcher
grep -q 'flushing Kafka producer' "$LOG" && ok "graceful shutdown" || bad "no graceful shutdown log"
smb 'echo during-downtime > /shares/data/in/while-down.txt'
start_watcher
wait_for_key in/while-down.txt 30 && expect_once in/while-down.txt || bad "no event for in/while-down.txt"
events | grep 'while-down' | grep -q '"source":"reconcile"' && ok "detected via reconcile" || bad "while-down not from reconcile"
expect_once in/sub/new.pdf  # no re-emit after restart

say "5. Samba restart"
"${COMPOSE[@]}" restart samba >/dev/null
smb 'echo after-restart > /shares/data/in/after-samba-restart.txt'
wait_for_key in/after-samba-restart.txt 60 && expect_once in/after-samba-restart.txt || bad "no event after Samba restart"
grep -q 'SMB session failed, reconnecting' "$LOG" && ok "reconnect logged" || bad "no reconnect in log"

say "6. many files at once ($MANY)"
smb "mkdir -p /shares/data/bulk && cd /shares/data/bulk && i=0; while [ \$i -lt $MANY ]; do echo \$i > file-with-a-longish-name-\$i.dat; i=\$((i+1)); done"
end=$((SECONDS + 120)); got=0
while [ $SECONDS -lt $end ]; do
  got="$(events | awk -F'\t' '$1 ~ /^bulk\//' | sort -u | wc -l | tr -d ' ')"
  [ "$got" -ge "$MANY" ] && break
  sleep 3
done
[ "$got" -ge "$MANY" ] && ok "$got/$MANY bulk files reported" || bad "only $got/$MANY bulk files reported"
dups="$(events | awk -F'\t' '{print $1}' | sort | uniq -d | wc -l | tr -d ' ')"
[ "$dups" = 0 ] && ok "no duplicate events" || bad "$dups keys with duplicate events"
ovf="$(curl -s "localhost:$HTTP_PORT/metrics" | awk '/^smb_watcher_watcher_overflows_total/{print $2}')"
echo "  info watcher overflows: ${ovf:-0} (depends on the server's notify buffer)"

say "7. polling fallback (USE_WATCHER=false)"
stop_watcher
start_watcher USE_WATCHER=false POLL_INTERVAL_SECS=5
smb 'echo polled > /shares/data/in/polled.txt'
wait_for_key in/polled.txt 40 && expect_once in/polled.txt || bad "no event in polling mode"
events | grep 'polled.txt' | grep -q '"source":"reconcile"' && ok "detected via polling reconcile" || bad "polled.txt not from reconcile"
w="$(curl -s "localhost:$HTTP_PORT/metrics" | awk '/^smb_watcher_watcher_active/{print $2}')"
[ "$w" = 0 ] && ok "watcher_active=0" || bad "watcher_active=$w"

say "result"
if [ "$FAILS" = 0 ]; then echo "ALL PASSED"; else echo "$FAILS FAILED"; exit 1; fi
