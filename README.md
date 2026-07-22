# sparrow

[![test](https://github.com/balicat/sparrowcli/actions/workflows/test.yml/badge.svg)](https://github.com/balicat/sparrowcli/actions/workflows/test.yml)

**A terminal client for any Apache Arrow Flight / Flight SQL server.**
One static binary: browse the catalog, run SQL, stream Arrow onward.
Human-friendly on a TTY, machine-friendly in a pipe, **agent-friendly by design**.

Most Flight servers ship SDKs. Sometimes you just want to inspect one from a
terminal. And the pipe is first-class: **a table when you're reading, raw
Arrow IPC when you're piping — the same command does both.**

> **Status** &nbsp; ✔ works against five independent Flight SQL servers &nbsp;·&nbsp; ✔ [binaries for Linux · macOS · Windows](https://github.com/balicat/sparrowcli/releases)
> **Validated against** &nbsp; ✔ GizmoSQL (DuckDB) &nbsp; ✔ Sparrow Flight &nbsp; ✔ ROAPI (DataFusion) &nbsp; ✔ Dremio OSS &nbsp; ✔ InfluxDB 3 Core

## Quick start — four commands

```sh
# a live 136-million-row Flight SQL server, open for exactly this:
sparrow connect grpc+tls://flight.sparrowflight.io:443 --basic demo:demo

sparrow ls
sparrow info series_data
sparrow sql "SELECT series_id, COUNT(*) FROM series_data GROUP BY 1 LIMIT 5"
```

## Commands

| command | does | wire calls |
|---|---|---|
| `sparrow connect <uri>` | verify + save a profile | vendor probe via `GetSqlInfo`, `SELECT 1` fallback |
| `sparrow orient` | one-shot markdown map: vendor, every table, every schema | `GetSqlInfo` + `GetTables` w/ schemas |
| `sparrow ls [pattern]` | list tables; the pattern is a server-side SQL `LIKE` (`%`, `_`, case-sensitive) | `GetTables` — the one discovery RPC that works everywhere |
| `sparrow info <table>` | schema, catalog, row count | `GetTables` w/ schema; `LIMIT 0` fallback |
| `sparrow sql "<query>"` | run a statement (`-` = stdin, `-f query.sql` = file; `--stats` / `--ipc` stream anatomy; `--schema` = columns+types only; `--cost` estimates result size without streaming; `--budget 10MB\|5000rows\|30s` aborts a stream past a ceiling (exit 1); `--receipt r.json` writes a verifiable receipt of the result; `--bigint-as-string` for JS precision; [`--substrait plan.pb`](docs/substrait.md) executes a Substrait plan) | `CommandStatementQuery` → `GetFlightInfo` → `DoGet` |
| `sparrow query <table>` | build the one-liner SELECT for you: `--cols` `--where` `--order` `--limit`; everything else works like `sql` | same as `sql` |
| `sparrow head <table> [n]` | preview the first n rows (default 10) — the `SELECT * … LIMIT n` you keep typing | `Execute` → `DoGet` |
| `sparrow pull '<ticket>'` | **Direct Pull (1-RTT)**: a ready ticket straight to the server — no `GetFlightInfo`, no SQL (`doget` is a hidden alias). Flight SQL reads are two round trips by design; servers that accept client-made tickets (Sparrow: JSON `{"series": [...]}` or `{"sql": "…"}`) serve known pulls in one — measured 143 vs 224 ms for the same 10k-row series over the public internet, the 81 ms gap being exactly the saved round trip (the win is one RTT, so it shrinks to nothing on a LAN). `--accept-compression lz4` (the default) asks a negotiating server for a compressed wire — decoded transparently; `--dry-run` prints the final composed ticket (after that injection) and sends nothing; `doctor --server` probes which kind a server is | `DoGet` only |
| `sparrow ticket '<sql>'` \| `--series a,b` | emit a **reusable** pull ticket (JSON) — save it, replay it forever with `pull @file`. A client ticket is stateless (re-run fresh each `pull`); a GetFlightInfo handle is single-use | none (client-side) |
| `sparrow profile <table>` | per-column nulls %, approx-distinct, min, max — one server-side pass | one aggregate query |
| `sparrow doctor` | layered connection diagnosis — names the layer that breaks (`--server`: [Flight SQL conformance card](docs/conform.md) — 10 surface probes incl. IPC compression) | staged: DNS → TCP → TLS/ALPN → auth → `GetTables` → `SELECT 1` |
| `sparrow check <table>` | data doctor: nulls, duplicate keys, staleness, frozen series, outliers. `--strict` fails on warnings · `--fail-on keys,nulls` gates the exit on named checks only (the rest still report) · `--show-violations` emits offending keys+values · `--approx` = memory-safe (HLL) uniqueness · `--explain` echoes each stage's SQL · `--baseline prior.json` gates on regressions | server-side SQL aggregates — the table is never downloaded |
| `sparrow expect '<sql>' --eq N` \| `--rows 0` \| `--cols a,b` | assert something about a query and exit 1 if it fails — an agent's self-authored **data contract**. Scalar (`--eq/--ne/--gt/--lt/--ge/--le`, numeric-aware), row-count (`--rows/--rows-min/--rows-max/--empty/--nonempty`, wrapped in `COUNT(*)`), or shape (`--cols`, names in order); any combination, all must hold | one query (counts never materialize) |
| `sparrow verify <receipt.json>` | re-run a receipt's query and confirm the result fingerprint still matches (`sql --receipt r.json` writes one) — **provable provenance**: exit 0 = the number is real, exit 1 = changed/tampered. `-s` verifies the same query against another server | one server-side fingerprint aggregate |
| `sparrow replay <session.jsonl>` | re-run a recorded **investigation** and confirm every step reproduces (set `SPARROW_SESSION=file` and each read appends a fingerprinted step) — exit 1 if any drifted. `-s` replays the whole thing against another server | one fingerprint aggregate per step |
| `sparrow diff <table> --against <b>` | [drift gate](docs/diff.md): schema, `COUNT(*)`, `--time` bounds, numeric fingerprint vs a second server — exit 1 on drift | conservative aggregates on both sides; nothing downloaded |
| `sparrow audit` | [security surface](docs/audit.md): what client SQL can reach beyond queries — file reads, dir listing, writes, SSRF, config tamper, **catalog writes (CREATE/DROP)**. Exit 1 if exposed | benign probes (incl. a create-then-drop round-trip); run against a server you operate |
| `sparrow ping` | separate network latency from server latency, as percentiles | bare TCP connect vs a no-match `GetTables` on the warm channel |
| `sparrow feedback "msg"` | send feedback to the sparrow maintainers | HTTPS to sparrowflight.io — independent of whichever server you use |
| `sparrow completion bash\|zsh\|fish` | shell tab-completion script | — |
| `sparrow profiles` | list saved connections (`use <name>` / `rm <name>`) | — |

Auth — the two adapters that cover the whole tested landscape:

```sh
--basic user:pass                          # GizmoSQL, Dremio, Sparrow (API key as user
                                           # works; Bearer handoff adopted automatically)
--bearer TOKEN --header database=mydb      # InfluxDB 3 style: token + per-call metadata
```

TLS: `grpc://` plain, `grpc+tls://` verified, `--tls-skip-verify` for
self-signed. The CLI identifies the server by trying `GetSqlInfo` first, then
`SELECT version()` as a fallback — Dremio answers the second, InfluxDB the
first; between them, every server identifies itself.

## Direct Pull (1 RTT) — `pull`

A Flight SQL read is two round trips by design: `GetFlightInfo` to plan, then
`DoGet` to stream. A **Direct Pull** sends a client-made ticket straight to the
server and skips the plan — one round trip. The same WTI crude series, fetched
both ways — an ordinary query the server plans, and a Direct Pull:

```sh
# the ordinary way — a query the server plans (2 round trips)
sparrow sql "SELECT series_id, period, value FROM series_data WHERE series_id='PET.RWTC.D' ORDER BY period" -o table --max-rows 5

# a Direct Pull — a ready ticket straight to the server (1 round trip)
sparrow pull '{"series":["PET.RWTC.D"]}' -o table --max-rows 5
```

Both print the identical rows:

```
series_id   period    value
PET.RWTC.D  19860102  25.56
PET.RWTC.D  19860103  26
PET.RWTC.D  19860106  26.53
PET.RWTC.D  19860107  25.85
PET.RWTC.D  19860108  25.87
```

A ticket comes in two dialects — `{"series":[…]}` by key, or `{"sql":"…"}` for
an arbitrary read-only query:

```sh
sparrow pull '{"sql":"SELECT series_id, COUNT(*) AS n FROM series_data GROUP BY 1 ORDER BY n DESC LIMIT 3"}' -o table
```
```
series_id                      n
FRED.DFF                       13340
PET.EER_EPD2F_PE3_Y35NY_DPG.D  11117
PET.EER_EPD2F_PE1_Y35NY_DPG.D  11113
```

Default output is raw Arrow IPC when piped — so it composes (`| duckdb`,
`> series.arrows`) — and an aligned table on a TTY; `-o` picks explicitly.
`--stats` shows the round trip you skipped (`plan (skipped: 1-RTT)`) and the
codec you negotiated (see [Measure](#measure--is-it-the-network-or-the-server)).
`pull` is the same operation as the Flight `DoGet` RPC — which is why `doget`
still works as a hidden alias. Opaque-handle vendors — most Flight SQL servers —
don't accept client tickets; use `sql` there, and `sparrow doctor --server`
probes which kind a server is.

A client ticket is a **reusable, durable artifact** — the server re-runs it
fresh on every `pull`, so save one and replay it forever (it survives restarts;
a GetFlightInfo statement handle does not — that's single-use, consumed on its
first `DoGet`). `sparrow ticket` writes one for you, JSON-escaped:

```sh
sparrow ticket "SELECT period, value FROM series_data WHERE series_id='PET.RWTC.D'" > wti.ticket
sparrow pull @wti.ticket -o md          # replay it, 1 RTT each, as often as you like
sparrow ticket --series PET.RWTC.D,FRED.DFF --start 2020-01-01 > two.ticket
```

## Output — pick your consumer

```sh
sparrow sql "..."                    # TTY: aligned table · pipe: raw Arrow IPC
sparrow sql "..." -o md              # markdown table
sparrow sql "..." -o csv             # CSV (empty cell = NULL)
sparrow sql "..." -o jsonl           # one JSON object per row
sparrow sql "..." -o json            # JSON array
sparrow sql "..." -o data.parquet    # file sink: .parquet .csv .json .jsonl .arrow .md
```

JSON note: 64-bit integers are emitted as JSON numbers at full precision.
JavaScript's `JSON.parse` silently loses precision above 2^53 — use a
bigint-aware parser, or cast to text in SQL.

**In a pipe, the default is a raw Arrow IPC stream** — columnar data stays
columnar all the way:

```sh
sparrow sql "SELECT period, value FROM series_data WHERE series_id='PET.RWTC.D'" \
  | duckdb -c "LOAD arrow; SELECT COUNT(*), MIN(value), MAX(value) FROM read_arrow('/dev/stdin')"
# → 10217 · -36.98 · 145.31 — forty years of WTI without leaving Arrow
# (one-time setup: duckdb -c "INSTALL arrow FROM community" — read_arrow is a
#  community extension, so the explicit LOAD is required; it never autoloads)
```

## Security

```sh
# private CA + mTLS — for Flight deployments that require client certificates
sparrow connect grpc+tls://flight.corp:443 \
  --tls-ca ca.crt --tls-cert client.crt --tls-key client.key

# sealed exports — in-spec Parquet Modular Encryption (AES-GCM)
sparrow sql "SELECT ..." -o data.parquet --encrypt-key env:SPARROW_KEY
```

The encryption key is hex (16/24/32 bytes), `env:VAR`, or `file:path`.
DuckDB, Spark and pyarrow read the file back with the key — and refuse it
without. The exact DuckDB recipe (key handed over as **base64** of the same
bytes):

```sql
PRAGMA add_parquet_key('k', '<base64 of the key bytes>');
SELECT * FROM read_parquet('data.parquet', encryption_config = {footer_key: 'k'});
```

**Prefer 32-byte keys (64 hex digits).** DuckDB guesses whether the key
string is raw bytes or base64 *by its length* — and base64 of a 16/24-byte
key is exactly 24/32 characters, a valid raw-key length that DuckDB tries
first, ending in a spurious "AES tag differs" error. A 32-byte key encodes
to 44 characters and is unambiguous. (Found by an external tester driving
this CLI — thanks.) mTLS verified against an Envoy that requires client
certificates: no cert → refused (exit 2); cert → query runs.

## Doctor — when the connection doesn't work

Every connection failure looks the same from a client: "connection error."
`sparrow doctor` walks the stack — config → DNS → TCP → TLS → auth → Flight
SQL → round trip — and names the layer that breaks, with evidence:

```
$ sparrow doctor -s grpc+tls://fixture:31337 --basic user:pass
 ✓ config    profile "(ad-hoc)" · auth basic · TLS system roots
 ✓ dns       fixture → 192.168.132.91 (18 ms)
 ✓ tcp       connected to 192.168.132.91:31337 (2 ms)
 ✗ tls       tls: failed to verify certificate: x509: certificate signed by unknown authority
             wire presented: subject "localhost" · issuer "Norton Web/Mail Shield Untrusted Root"
             hint: if that issuer is not your server's CA, something between you and the
             server is intercepting TLS (antivirus HTTPS scanning, corporate proxy)
 · auth      not reached

3 ok · 0 warn · 1 fail — first failure at tls
```

That capture is real — a self-signed fixture that "wouldn't verify" turned out
to be local antivirus re-signing the connection, and doctor shows the swapped
certificate straight off the wire. On a healthy endpoint it reports the TLS
version, ALPN (gRPC needs `h2`; doctor says so when a proxy won't negotiate
it), certificate issuer and expiry, the auth handshake, the vendor, and a
timed round trip. `-o json` for scripts; exit `2` if any layer fails.

## Measure — is it the network or the server?

`sparrow ping` runs two round trips per round — a bare TCP connect (pure
network) and a lightweight RPC on the warm, authenticated channel (network +
server) — and summarizes the percentiles. The gap between the two is the
server:

```
$ sparrow ping -n 5 -s grpc+tls://flight.sparrowflight.io:443 --basic demo:demo
round  1   tcp    75.9 ms   rpc    86.1 ms
...
            min     p50     p95     max
tcp        61.3    63.4    75.9    75.9  ms
rpc        75.4    81.4    86.1    86.1  ms   (5/5 ok)

≈ network 63.4 ms + server 18.0 ms (medians)
```

`sparrow sql --stats` breaks a query's wall clock into its anatomy — plan,
first byte, stream — plus rows, bytes that actually crossed the wire (counted
at the gRPC layer), throughput, pacing, and the per-column type/encoding
breakdown. Half a million rows over the public internet:

```
$ sparrow sql "SELECT * FROM series_data LIMIT 500000" --stats > /dev/null
── query stats ─────────────────────────
plan (GetFlightInfo)      79 ms
first byte               404 ms
stream (DoGet)          2304 ms
total                   2384 ms
rows       500,000 in 245 batches · rows/batch p50 2,048 (min 288 · max 2,048)
wire       22.5 MB received · decodes to 22.4 MB (1.0×) · no body compression declared
speed      78 Mbit/s over the stream
pacing     gaps p50 4.2 ms · p95 22.7 ms · max 164.7 ms — 82% of the stream is
           waiting (paced upstream: sender or network stalls between batches)
column     type     encoding  nulls  decoded
series_id  utf8     plain     0      13.2 MB (59%)
period     utf8     plain     0      5.2 MB (23%)
value      float64  plain     0      4.0 MB (18%)
```

That's the stream's full anatomy: the server's batch signature (2,048-row
chunks — DuckDB's vector size showing through), whether the wire is actually
compressed — read from the IPC message header's declared codec, so a
compressed stream prints `codec lz4_frame` (or `zstd`) with the ratio
corroborating it — what
every column arrives *as* (type, arrow-level encoding, nulls, share of the
bytes), and whether the stream is paced by the wire or by gaps upstream —
measured excluding the client's own write time, so a slow local sink doesn't
skew the verdict. Run the same pull against two servers and the differences
name themselves.

Prefer the raw view? `sql --ipc` prints the message-by-message IPC manifest
instead: type, rows, body bytes, declared codec, custom metadata.

The example above reads `no body compression declared` because a `sql` query
is a 2-RTT read — the opaque statement ticket can't negotiate a codec. To
*request* a compressed wire, use a Direct Pull (Sparrow serving nodes accept
JSON tickets): `sparrow pull '{"series":["…"]}' --accept-compression lz4` (lz4
is on by default; `--accept-compression ""` opts out). A negotiating server
compresses only for a codec the client lists, `arrow-go` decodes it
transparently, and the same `--stats`/`--ipc` view then prints `codec
lz4_frame` with the ratio. `sparrow doctor --server` probes whether a server
offers it at all.

```
$ sparrow pull '{"series":["PET.RWTC.D"]}' --stats > /dev/null
plan (skipped: 1-RTT)      0 ms
rows       10,217 in 5 batches · rows/batch p50 2,048
wire       172.5 KB received · decodes to 347.4 KB (2.0×) · codec lz4_frame
```

Same 10k-row series, half the bytes on the wire — the server compressed it
because the client (`arrow-go` here) advertised `lz4`, and the ratio comes back
in the same stats line. Compression trades CPU for bandwidth: a clear win over
a wide-area link, a wash or worse on a fast LAN where the wire was never the
bottleneck — so it is negotiated, never forced.

Stats go to stderr, so they compose with every output format and pipe.
Every benchmark number this project publishes is reproducible with this flag.
`ping -o json` for scripts; both work against any Flight SQL server.

## Check — a doctor for the data itself

`sparrow check <table>` runs a statistical health screen **server-side** —
every check is one conservative SQL aggregate, the table is never downloaded,
and anything a dialect rejects degrades to a skip instead of aborting:

```
$ sparrow check checkme --key k --max-age 7d
 ✓ table     3 columns · key k · time t (auto-detected)
 ✓ rows      60
 ✓ nulls     v 8.3%
 ✗ keys      1 duplicated (k, t) groups
             e.g. DUP
 ⚠ time      t spans 2026-06-01 → 2026-06-30 · newest point 13.7 days old
             hint: older than --max-age 7d — is the feed still running?
 ✓ coverage  5 entities · rows per entity: min 1 · avg 12 · max 30
 ⚠ frozen    1 entities have a constant v across ≥10 observations
             e.g. FLAT
 ✓ numeric   v: min 1 · max 900000 · avg 16432.25

5 ok · 2 warn · 1 fail — checked in 0.0 s (10 queries, server-side)
```

`--key` names the entity key (uniqueness is checked on *(key, time)* when
`--time` is set, and temporal columns are auto-detected). Duplicate keys and
NULLs in key columns are failures and exit `1` — `sparrow check t --key id &&
deploy` works as a data gate in CI (`--strict` widens the gate to warnings,
and a sub-check the server couldn't execute is an `!` error, never a silent
pass). Staleness, frozen series (a constant
value across ≥10 observations — a stuck feed's signature), dead columns and
σ-outliers are warnings. `-o json` for pipelines and agents.

## Prove it — verifiable receipts

When an agent (or a person) reports a number from the data, a **receipt** makes
it checkable instead of taken on faith:

```sh
sparrow sql "SELECT count(*) FROM series_data WHERE value < 0" -o md --receipt neg.json
sparrow verify neg.json
#  ✓ server    EnergyScope 1.0
#  ✓ rows       10,217
#  ✓ digest     matches
```

`--receipt` writes a manifest — the query, the server's identity, a timestamp,
and an **order-independent content fingerprint** of the result (a row count plus
two independent digests, computed server-side via `hash()` aggregates, so
nothing extra is downloaded and row order doesn't matter). `sparrow verify`
re-runs the query and confirms the fingerprint, the result's column shape, and —
against the receipt's own endpoint — that the server still identifies as the one
recorded: **exit 0** = the number is real, **exit 1** = the data/shape/server
changed or the receipt was tampered (a fabricated row count, digest, columns, or
vendor is caught), **exit 2** = unreachable/auth, **exit 3** = malformed receipt.
`verify -s <other>` runs the same query against a different server — a clean way
to ask "do two engines agree?"
(they do here: `flight` and `flight2` return an identical digest over the same
136M-row snapshot). A query that isn't deterministic server-side (`now()`,
`random()`) won't verify — which is the honest result.

Compose it with [`expect`](#commands): **assert** a contract, **receipt** the
result, let a third party **verify** it. Analysis you can check, not just trust.

## Reproducible investigations — record & replay

A receipt proves one number. A **session** proves a whole exploration. Set
`SPARROW_SESSION=<file>` and every read appends a replayable step — the query,
the endpoint, the row count, and (for SQL) the same content fingerprint:

```sh
export SPARROW_SESSION=probe.jsonl
sparrow sql "SELECT count(*) FROM series_data WHERE value < 0"
sparrow sql "SELECT max(value) AS wti_high FROM series_data WHERE series_id='PET.RWTC.D'"
unset SPARROW_SESSION

sparrow replay probe.jsonl
#  ✓ step 1  sql   reproduces
#  ✓ step 2  sql   reproduces
#  2/2 verifiable steps reproduce
```

`sparrow replay` re-runs each SQL step against its endpoint and diffs the
fingerprint — **exit 0** if the investigation still holds, **exit 1** if any
step drifted (it names which, and whether the row count or the content changed).
`replay -s <other>` runs the entire investigation against a different server —
"how I arrived at this" turned into a regression test you (or a reviewer, or CI)
can re-run. Pull/head steps are recorded for the narrative but marked rows-only
(no server-side fingerprint). It's the reusable-ticket idea lifted from one
query to a whole exploration.

## Install

Download a binary from [the releases page](https://github.com/balicat/sparrowcli/releases)
(Linux, macOS and Windows; amd64 + arm64), unpack it and put `sparrow` on your
PATH. Checksums included. Or install with Go, or build from source:

```sh
go install github.com/balicat/sparrowcli@latest   # installs as `sparrowcli` — rename if you like

go build -o sparrow .        # Go ≥ 1.25; pure Go, no cgo — trivially cross-compiles
GOOS=windows go build -o sparrow.exe .
```

## For AI agents (Claude Code, etc.)

**`sparrow agent`** prints a complete, self-contained manual (markdown) for
driving the CLI — every command, the `sql`-vs-`pull` decision, the output and
exit-code conventions, how to call server-advertised macros like full-text
search. Point an agent at it once (`sparrow agent > SPARROW.md`) and it can
operate the tool with no other docs. `sparrow agent --json` emits the same
surface as a machine-readable capability catalog (commands, flags, exit codes,
ticket dialects) for programmatic bootstrap against an unknown version. The
rest of this section is the summary.

AI agents don't need a Flight client library — they can just call the CLI.
**One command maps a Flight server** — vendor, tables, schemas, as markdown:

```sh
sparrow orient
```

Then query with results the agent reads natively:

```sh
sparrow info series_data                          # row count for one table
sparrow sql "SELECT ... LIMIT 20" -o md           # arbitrary query, readable results
echo "SELECT ..." | sparrow sql - -o md           # SQL via stdin — no shell-quoting battles
sparrow pull '{"series":["PET.RWTC.D"]}' -o md     # a known series in ONE round trip
```

Conventions agents can rely on:

- **Two ways to read the same data.** `sparrow sql "<query>"` plans an
  arbitrary query — two round trips (`GetFlightInfo` then `DoGet`). When you
  already know exactly what you want, `sparrow pull '<ticket>'` sends a ready
  ticket straight to the server in **one** round trip: a `{"series":[…]}` key
  or a `{"sql":"…"}` string, lz4-compressed by default. `sparrow help pull`
  prints the ticket dialects; the server also advertises them in-band via
  `GetSqlInfo`. Opaque-handle vendors reject client tickets — use `sql` there
  (`sparrow doctor --server` says which kind a server is). Rule of thumb:
  **exploring → `sql`; a known series or a fixed query you'll repeat → `pull`.**

- `-o md` / `-o jsonl` / `-o csv` are stable, parseable stdout formats — no
  ANSI, no decoration; row-count and timing summaries go to **stderr**.
- Exit codes: `0` ok · `1` query error · `2` connection/auth · `3` usage —
  branch on "server down" vs "my SQL was wrong".
- On exit `2`, run `sparrow doctor -o json` — a layer-by-layer diagnosis
  (DNS, TCP, TLS/ALPN, auth) as structured JSON, instead of guessing.
- `sparrow ping -o json` (latency percentiles, network-vs-server split) and
  `sql --stats` (timing/throughput anatomy on stderr) make measurements
  scriptable too.
- `sparrow check <table> --key id -o json` screens a dataset's health without
  downloading it — exit `1` means findings, so it gates pipelines.
- `sql --ipc` reveals the stream's raw IPC manifest — every message's type
  (schema / dictionary / record batch), rows, body bytes, declared codec and
  custom-metadata count — wire-level introspection without a packet capture.
- **Found a bug or have an idea? `sparrow feedback "..." --from your-name`**
  delivers it to the sparrow maintainers directly — independent of whichever
  Flight server you're connected to, so it works even when the server is the
  problem. Agents are explicitly welcome to use it.
- `-o md` **to stdout** caps at 1,000 rows by default so a careless `SELECT *`
  can't flood a context window (the true total reports on stderr; `--max-rows`
  overrides). File sinks and data formats (csv/jsonl/json/arrow/parquet)
  always emit everything.
- Prefer `LIMIT` in SQL — `--max-rows` still downloads the full result.
- Profiles live in `~/.sparrow/config.json`; `sparrow profiles use <name>`
  switches the default, `-s grpc+tls://host:port --basic u:p` works ad-hoc.

## The landscape (as of July 2026)

Other CLIs can reach a Flight SQL server — none preserve Arrow end-to-end:

| | scope | Arrow stays Arrow? |
|---|---|---|
| `flight_sql_client` ([apache/arrow-rs](https://github.com/apache/arrow-rs/blob/main/arrow-flight/README.md)) | any Flight SQL server | ✗ — "basic" example binary, one-shot RPC commands, text out |
| [`timvw/arrow-flight-sql-client`](https://github.com/timvw/arrow-flight-sql-client) | any Flight SQL server | ✗ — small RPC-mirror CLI, text out |
| [`usql`](https://github.com/xo/usql) | 40+ databases; Flight SQL as one driver | ✗ — excellent universal shell, but results flatten through `database/sql` to rows and text |
| `bendsql` (né "Arrow CLI") | Databend only | — what happens when an Arrow CLI grows up inside one vendor |
| **`sparrow`** | **any Flight SQL server** | **✓ — columnar from Flight server to downstream process: raw IPC pipe, parquet sinks, typed formats** |

The gap sparrow fills isn't "a CLI exists" — it's Arrow-native ergonomics:
catalog discovery over the Flight RPCs, profiles, `orient`, output that follows
the consumer, and conventions agents can script against.

## The Sparrow family

One transport, many clients: [Sparrow](https://sparrowflight.io) (the Flight
server) · [sparrowJS](https://github.com/balicat/sparrowjs) (the browser) ·
[sparrowXL](https://sparrowflight.io/excel) (Excel) ·
[sparrowMCP](https://sparrowflight.io/mcp) (AI agents) · **sparrowCLI** (the
terminal).

---

Dialect note: your SQL passes through untouched — quirks are the server's
business (e.g. Dremio rejects aliases on FROM-less SELECTs; the CLI reports
the server's error verbatim and exits 1).

## License

[Apache-2.0](LICENSE)
