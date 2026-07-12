# sparrow

**A terminal client for any Apache Arrow Flight / Flight SQL server.**
One static binary: browse the catalog, run SQL, stream Arrow onward.
Human-friendly on a TTY, machine-friendly in a pipe, **agent-friendly by design**.

Most Flight servers ship SDKs. Sometimes you just want to inspect one from a
terminal. And the pipe is first-class: **a table when you're reading, raw
Arrow IPC when you're piping — the same command does both.**

> **Status** &nbsp; ✔ works against four independent Flight SQL servers &nbsp;·&nbsp; ⚠ pre-release, no binaries published yet
> **Validated against** &nbsp; ✔ GizmoSQL (DuckDB) &nbsp; ✔ Sparrow Flight &nbsp; ✔ ROAPI (DataFusion) &nbsp; ✔ Dremio OSS*

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
| `sparrow ls [pattern]` | list tables | `GetTables` — the one discovery RPC that works everywhere |
| `sparrow info <table>` | schema, catalog, row count | `GetTables` w/ schema; `LIMIT 0` fallback |
| `sparrow sql "<query>"` | run a statement | `CommandStatementQuery` → `GetFlightInfo` → `DoGet` |
| `sparrow profiles` | list saved connections | — |

Auth: `--basic user:pass` (API key as user works; Bearer handoff adopted
automatically, GizmoSQL-style). TLS: `grpc://` plain, `grpc+tls://` verified,
`--tls-skip-verify` for self-signed.

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
sparrow sql "SELECT * FROM series_data WHERE series_id='PET.RWTC.D'" \
  | duckdb -c "SELECT MAX(value) FROM read_arrow('/dev/stdin')"
```

## For AI agents (Claude Code, etc.)

AI agents don't need a Flight client library — they can just call the CLI.
Three commands orient one completely; `-o md` returns tables it can read
natively:

```sh
sparrow ls -o md                 # what tables exist
sparrow info series_data         # schema + row count
sparrow sql "SELECT ... LIMIT 20" -o md   # readable results
```

Conventions agents can rely on:

- `-o md` / `-o jsonl` / `-o csv` are stable, parseable stdout formats — no
  ANSI, no decoration; row-count and timing summaries go to **stderr**.
- Exit codes: `0` ok · `1` query/connection error · `3` usage.
- `--max-rows N` caps emitted rows (the total still reports on stderr).
  Prefer `LIMIT` in SQL — `--max-rows` still downloads the full result.
- Profiles live in `~/.sparrow/config.json`; `-s <profile>` selects one,
  `-s grpc+tls://host:port --basic u:p` works ad-hoc.

## Build from source

```sh
go build -o sparrow .        # Go ≥ 1.25; pure Go, no cgo — trivially cross-compiles
GOOS=windows go build -o sparrow.exe .
```

## The Sparrow family

One transport, many clients: [Sparrow](https://sparrowflight.io) (the Flight
server) · [sparrowJS](https://github.com/balicat/sparrowjs) (the browser) ·
[sparrowXL](https://sparrowflight.io/excel) (Excel) ·
[sparrowMCP](https://sparrowflight.io/mcp) (AI agents) · **sparrowCLI** (the
terminal).

---

\* Dremio: connect/ls/sql validated 2026-07-09 via the same auth + discovery
path (see [sparrowflight.io/cli](https://sparrowflight.io/cli)); its SQL
dialect quirks are its own business — your SQL passes through untouched.

## License

[Apache-2.0](LICENSE)
