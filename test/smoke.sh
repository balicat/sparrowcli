#!/usr/bin/env bash
# End-to-end smoke test against a live GizmoSQL fixture (TLS, self-signed).
# Exercises every command and the full exit-code contract with a real server.
#
#   BIN=./sparrow URI=grpc+tls://localhost:31337 \
#   USERPASS=gizmosql_username:gizmosql_password bash test/smoke.sh
#
# pyarrow checks (parquet sink, raw IPC pipe) run when python3+pyarrow exist.
set -u

BIN=${BIN:-./sparrow}
URI=${URI:-grpc+tls://localhost:31337}
USERPASS=${USERPASS:-gizmosql_username:gizmosql_password}
export HOME=$(mktemp -d) # isolate ~/.sparrow from the real machine
OUT=$(mktemp) ERR=$(mktemp)

fails=0
t() { # t <name> <want_exit> <cmd...>
  local name=$1 want=$2
  shift 2
  "$@" >"$OUT" 2>"$ERR"
  local got=$?
  if [ "$got" != "$want" ]; then
    echo "FAIL $name: exit $got, want $want"
    sed 's/^/     | /' "$ERR" | head -4
    fails=$((fails + 1))
  else
    echo "ok   $name"
  fi
}
has() { # has <name> <pattern> [file]
  grep -q "$2" "${3:-$OUT}" || { echo "FAIL $1: output missing '$2'"; fails=$((fails + 1)); }
}

# ── connect + profiles ───────────────────────────────────────────────────
t connect 0 "$BIN" connect "$URI" --basic "$USERPASS" --tls-skip-verify
has connect-vendor "connected"
t profiles 0 "$BIN" profiles
has profiles-default "default"

# ── fixture table (fresh container: writable default db) ────────────────
t create 0 "$BIN" sql "CREATE TABLE smoke AS SELECT r AS id, r * 1.5 AS val FROM range(1000) t(r)" -o csv

# ── discovery ────────────────────────────────────────────────────────────
t ls 0 "$BIN" ls
t info 0 "$BIN" info smoke
has info-rows "rows: 1,000"
t orient 0 "$BIN" orient
has orient-table "smoke"

# ── query + formats ──────────────────────────────────────────────────────
t sql-csv 0 "$BIN" sql "SELECT COUNT(*) AS n FROM smoke" -o csv
has csv-count "^1000$"
t sql-md 0 "$BIN" sql "SELECT id FROM smoke ORDER BY id LIMIT 5" -o md
has md-pipe "^| id |"
t sql-json 0 "$BIN" sql "SELECT id, val FROM smoke ORDER BY id LIMIT 3" -o json
if command -v python3 >/dev/null; then
  python3 -c "import json,sys; r=json.load(open('$OUT')); assert len(r)==3, r" \
    || { echo "FAIL json-parse"; fails=$((fails + 1)); }
fi
t sql-jsonl 0 "$BIN" sql "SELECT id FROM smoke LIMIT 4" -o jsonl
[ "$(wc -l <"$OUT")" = 4 ] || { echo "FAIL jsonl-lines"; fails=$((fails + 1)); }
t sql-stdin 0 bash -c "echo 'SELECT 7 AS seven' | $BIN sql - -o csv"
has stdin-result "^7$"
t sql-stats 0 "$BIN" sql "SELECT r FROM range(5000) t(r)" -o csv --stats
has stats-block "query stats" "$ERR"
has stats-batches "rows/batch" "$ERR"
has stats-anatomy "encoding" "$ERR"
has stats-pacing "pacing" "$ERR"
has stats-codec "no body compression declared" "$ERR"

# ── the md cap: stdout capped at 1000, explicit file sink gets everything ─
t md-cap 0 "$BIN" sql "SELECT r FROM range(1500) t(r)" -o md
[ "$(grep -c '^|' "$OUT")" = 1002 ] || { echo "FAIL md-cap: $(grep -c '^|' "$OUT") lines"; fails=$((fails + 1)); }
MDF=$(mktemp -u --suffix=.md)
t md-file 0 "$BIN" sql "SELECT r FROM range(1500) t(r)" -o "$MDF"
[ "$(grep -c '^|' "$MDF")" = 1502 ] || { echo "FAIL md-file: $(grep -c '^|' "$MDF") lines"; fails=$((fails + 1)); }
rm -f "$MDF"

# ── check: the data doctor ───────────────────────────────────────────────
t check-clean 0 "$BIN" check smoke --key id
has check-rows "rows.*1,000"
t check-seed 0 "$BIN" sql "CREATE TABLE checkme AS
SELECT 'OK' AS k, (DATE '2026-06-01' + INTERVAL (r) DAY)::DATE AS t, 100 + r * 1.5 AS v FROM range(30) x(r)
UNION ALL SELECT 'FLAT', (DATE '2026-06-01' + INTERVAL (r) DAY)::DATE, 7.0 FROM range(15) x(r)
UNION ALL SELECT 'DUP', DATE '2026-06-05', 1.0 FROM range(2) x(r)" -o csv
t check-dirty 1 "$BIN" check checkme --key k
has check-dup "duplicated (k, t)"
has check-frozen "FLAT"
t check-json 1 "$BIN" check checkme --key k -o json
if command -v python3 >/dev/null; then
  python3 -c "import json; r=json.load(open('$OUT')); assert r['ok'] is False and r['table']=='checkme', r" \
    || { echo "FAIL check-json-shape"; fails=$((fails + 1)); }
fi
t check-missing-table 1 "$BIN" check no_such_table
t check-usage 3 "$BIN" check

# ── diagnostics ──────────────────────────────────────────────────────────
t doctor 0 "$BIN" doctor
t doctor-json 0 "$BIN" doctor -o json
if command -v python3 >/dev/null; then
  python3 -c "import json; r=json.load(open('$OUT')); assert r['ok'] is True, r" \
    || { echo "FAIL doctor-json-ok"; fails=$((fails + 1)); }
fi
t ping 0 "$BIN" ping -n 2 -interval 50ms
t ping-json 0 "$BIN" ping -n 2 -interval 50ms -o json

# ── the exit-code contract, end to end ───────────────────────────────────
t exit1-bad-sql 1 "$BIN" sql "SELECT definitely_not_a_column FROM smoke"
t exit2-dead-server 2 "$BIN" sql "SELECT 1" -s grpc://localhost:9 -o csv
t exit2-bad-auth 2 "$BIN" sql "SELECT 1" -s "$URI" --basic wrong:creds --tls-skip-verify -o csv
t exit3-bad-flag 3 "$BIN" sql --no-such-flag "SELECT 1"
t exit3-missing-arg 3 "$BIN" info
t exit0-help 0 "$BIN" sql -h
t exit3-bare 3 "$BIN"

# ── arrow-native guarantees (pyarrow) ────────────────────────────────────
if python3 -c "import pyarrow" 2>/dev/null; then
  PQ=$(mktemp -u --suffix=.parquet)
  t parquet-sink 0 "$BIN" sql "SELECT id, val FROM smoke" -o "$PQ"
  python3 -c "import pyarrow.parquet as pq; assert pq.read_table('$PQ').num_rows==1000" \
    && echo "ok   parquet-readback" || { echo "FAIL parquet-readback"; fails=$((fails + 1)); }
  rm -f "$PQ"
  "$BIN" sql "SELECT id FROM smoke" >"$OUT" 2>/dev/null
  python3 -c "
import pyarrow.ipc as ipc
assert ipc.open_stream(open('$OUT','rb')).read_all().num_rows == 1000" \
    && echo "ok   ipc-pipe" || { echo "FAIL ipc-pipe: pipe is not a valid Arrow IPC stream"; fails=$((fails + 1)); }
else
  echo "skip pyarrow checks (pyarrow not installed)"
fi

# ── encrypted parquet round trip (pyarrow can't read it without the key —
#    DuckDB verifies WITH the key elsewhere; here we assert seal-ness) ─────
if python3 -c "import pyarrow" 2>/dev/null; then
  EPQ=$(mktemp -u --suffix=.parquet)
  t sealed-parquet 0 "$BIN" sql "SELECT id FROM smoke LIMIT 10" -o "$EPQ" --encrypt-key 000102030405060708090a0b0c0d0e0f
  python3 -c "
import pyarrow.parquet as pq
try:
    pq.read_table('$EPQ'); raise SystemExit('readable without key')
except SystemExit: raise
except Exception: pass" \
    && echo "ok   sealed-refuses-keyless" || { echo "FAIL sealed-refuses-keyless"; fails=$((fails + 1)); }
  rm -f "$EPQ"
fi

# ── cleanup ──────────────────────────────────────────────────────────────
"$BIN" sql "DROP TABLE smoke" -o csv >/dev/null 2>&1
"$BIN" sql "DROP TABLE checkme" -o csv >/dev/null 2>&1
rm -f "$OUT" "$ERR"

echo
if [ "$fails" -gt 0 ]; then
  echo "SMOKE: $fails failure(s)"
  exit 1
fi
echo "SMOKE: all green"
