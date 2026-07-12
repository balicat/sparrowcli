# sparrow

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
| `sparrow ls [pattern]` | list tables | `GetTables` — the one discovery RPC that works everywhere |
| `sparrow info <table>` | schema, catalog, row count | `GetTables` w/ schema; `LIMIT 0` fallback |
| `sparrow sql "<query>"` | run a statement (`-` = stdin, `-f query.sql` = file) | `CommandStatementQuery` → `GetFlightInfo` → `DoGet` |
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

## Output — pick your consumer

```sh
sparrow sql "..."                    # TTY: aligned table · pipe: raw Arrow IPC
sparrow sql "..." -o md              # markdown table
sparrow sql "..." -o csv             # CSV (empty cell = NULL)
sparrow sql "..." -o jsonl           # one JSON object per row
sparrow sql "..." -o json            # JSON array
sparrow sql "..." -o data.parquet    # file sink: .parquet .csv .json .jsonl .arrow .md
```

**In a pipe, the default is a raw Arrow IPC stream** — columnar data stays
columnar all the way:

```sh
sparrow sql "SELECT period, value FROM series_data WHERE series_id='PET.RWTC.D'" \
  | duckdb -c "SELECT COUNT(*), MIN(value), MAX(value) FROM read_arrow('/dev/stdin')"
# → 10217 · -36.98 · 145.31 — forty years of WTI without leaving Arrow
# (one-time setup: duckdb -c "INSTALL arrow FROM community")
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
without. Verified against an Envoy that requires client certificates: no
cert → refused (exit 2); cert → query runs.

## Install

Download a binary from [the releases page](https://github.com/balicat/sparrowcli/releases)
(Linux, macOS and Windows; amd64 + arm64), unpack it and put `sparrow` on your
PATH. Checksums included. Or build from source:

```sh
go build -o sparrow .        # Go ≥ 1.25; pure Go, no cgo — trivially cross-compiles
GOOS=windows go build -o sparrow.exe .
```

## For AI agents (Claude Code, etc.)

AI agents don't need a Flight client library — they can just call the CLI.
**One command maps a Flight server** — vendor, tables, schemas, as markdown:

```sh
sparrow orient
```

Then query with results the agent reads natively:

```sh
sparrow info series_data                  # row count for one table
sparrow sql "SELECT ... LIMIT 20" -o md   # readable results
echo "SELECT ..." | sparrow sql - -o md   # SQL via stdin — no shell-quoting battles
```

Conventions agents can rely on:

- `-o md` / `-o jsonl` / `-o csv` are stable, parseable stdout formats — no
  ANSI, no decoration; row-count and timing summaries go to **stderr**.
- Exit codes: `0` ok · `1` query error · `2` connection/auth · `3` usage —
  branch on "server down" vs "my SQL was wrong".
- `-o md` caps at 1,000 rows by default so a careless `SELECT *` can't flood
  a context window (the true total reports on stderr; `--max-rows` overrides).
  Data formats (csv/jsonl/json/arrow/parquet) always emit everything.
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
