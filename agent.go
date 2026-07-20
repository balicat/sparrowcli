// agent — `sparrow agent` prints a complete, self-contained operator's manual
// for an AI agent (Claude Code et al.) driving the CLI. One command, one
// markdown document, no server required: everything an agent needs to discover
// a Flight server, read data the fast way, parse the output, and recover from
// errors. Point an agent at `sparrow agent` once and it can operate the tool.
package main

import (
	"fmt"
	"strings"
)

func cmdAgent(args []string) error {
	fs := newFlagSet("agent", `usage: sparrow agent
Print a complete agent-ready manual (markdown) for driving sparrow, to stdout.
Self-contained — no server connection needed. Save it or pipe it:
  sparrow agent > SPARROW.md
For a specific server's live tables and macros, run `+"`sparrow orient`"+` after.`)
	parseFlags(fs, args)
	fmt.Print(strings.Replace(agentGuide, "{{VERSION}}", versionString(), 1))
	return nil
}

const agentGuide = "# sparrow — agent guide (" + "{{VERSION}}" + `)

` + "`sparrow`" + ` is a command-line client for **any Apache Arrow Flight / Flight SQL
server**. One binary: discover a server, run SQL, pull data, export or pipe
columnar Arrow. You (an AI agent) drive it entirely from the shell — no client
library, no code. Results come back as text you can read (` + "`-o md`" + `) or as raw
Arrow you can pipe onward.

## Orient yourself (run these first)

` + "```sh" + `
sparrow connect grpc+tls://host:port --basic user:pass   # save a profile (first one = default)
sparrow orient                                           # ONE markdown map: vendor, every table, every schema
sparrow ls [pattern]                                     # list tables; pattern is a server-side SQL LIKE (%, _)
sparrow info <table>                                     # schema, catalog, row count for one table
` + "```" + `

` + "`orient`" + ` is the single best first command — it tells you the vendor and the
whole catalog in one shot. Do it before guessing table or column names.

## Read data — two ways, pick deliberately

- **` + "`sparrow sql \"SELECT ...\"`" + `** — run an arbitrary query. Two round trips
  (plan, then stream). Use for **exploring, aggregates, joins, anything ad-hoc**.
  ` + "`-` " + `reads SQL from stdin (no shell-quoting battles); ` + "`-f q.sql`" + ` from a file.
- **` + "`sparrow pull '<ticket>'`" + `** — a **Direct Pull**: a ready ticket straight to
  the server, **one round trip** (skips planning). Use when you **already know
  exactly what you want** — a known series, or a fixed query you'll repeat.
  - ticket dialects (Sparrow servers): ` + "`{\"series\":[\"ID\", ...]}`" + ` or ` + "`{\"sql\":\"SELECT ...\"}`" + `
  - lz4-compressed on the wire by default (` + "`--accept-compression \"\"`" + ` disables it)
  - not every server accepts client tickets — ` + "`sparrow doctor --server`" + ` says which;
    use ` + "`sql`" + ` on the ones that don't.

**Rule of thumb: exploring → ` + "`sql`" + ` · a known series or a fixed repeated query → ` + "`pull`" + `.**

## Output — built for programmatic reading

- ` + "`-o md`" + ` / ` + "`-o jsonl`" + ` / ` + "`-o csv`" + ` — **stable, parseable stdout**: no ANSI, no
  decoration. Prefer ` + "`-o md`" + ` when you'll read the rows yourself, ` + "`-o jsonl`" + ` to parse.
- **In a pipe, the default is raw Arrow IPC** — composes with ` + "`duckdb`" + `, ` + "`pyarrow`" + `,
  anything Arrow-aware. Redirect to a file with a data extension to write it:
  ` + "`-o out.parquet`" + ` / ` + "`.csv`" + ` / ` + "`.arrow`" + `.
- **Row-count and timing summaries go to STDERR**, never stdout — so they can't
  corrupt your ` + "`-o`" + ` output. Read stdout for data, stderr for meta.
- ` + "`-o md`" + ` to stdout **caps at 1000 rows** so a careless ` + "`SELECT *`" + ` can't flood
  your context (the true total prints on stderr; ` + "`--max-rows`" + ` overrides).
- ` + "`sql --schema`" + ` prints columns + Arrow types and **fetches no rows** — cheap
  shape-check before a big pull.

## Server-advertised functions (e.g. full-text search)

Some servers expose **table MACROs** — they appear in ` + "`ls`" + `/` + "`orient`" + ` with type
` + "`MACRO`" + `. A MACRO is a **function you CALL with arguments**, not a table you
` + "`SELECT * FROM`" + ` bare (that errors — it needs args).

- The Sparrow/EnergyScope demo advertises ` + "`search_meta`" + ` — **BM25 full-text
  search** over the whole series catalog (millions of series):
  ` + "```sh" + `
  sparrow sql "SELECT * FROM search_meta('jet fuel europe', lim := 20)" -o md
  ` + "```" + `
  Returns ` + "`series_id, name, description, score, total_matches`" + ` — where
  ` + "`total_matches`" + ` is the **pre-LIMIT** hit count, so truncation is explicit.
  Optional args: ` + "`lim := N`" + ` (cap, default 50), ` + "`dedup := true`" + ` (collapse
  unit-variant duplicates). It composes with ` + "`JOIN`" + `/` + "`WHERE`" + ` like any table.
- To learn **any** macro's argument names on a DuckDB-backed server:
  ` + "```sh" + `
  sparrow sql "SELECT parameters FROM duckdb_functions() WHERE function_name='search_meta'"
  ` + "```" + `

## When something breaks — exit codes are your branch

- ` + "`0`" + ` ok · ` + "`1`" + ` query error (your SQL/ticket was wrong) · ` + "`2`" + ` connection/auth
  (server down or bad creds) · ` + "`3`" + ` usage (you called sparrow wrong).
- **On exit 2**, don't guess — run ` + "`sparrow doctor -o json`" + `: a layer-by-layer
  diagnosis (dns → tcp → tls → auth) as structured JSON naming the layer that broke.
- Branch on the code: ` + "`1`" + ` means fix your query, ` + "`2`" + ` means fix the connection.

## Measure & verify (all reproducible, all scriptable)

` + "```sh" + `
sparrow ping -o json                       # network vs server latency, percentiles
sparrow sql "..." --stats                  # plan/first-byte/stream ms, wire bytes, codec, throughput (stderr)
sparrow check <table> --key id -o json     # data health: nulls, dup keys, staleness; exit 1 = findings (gates CI)
sparrow diff <t> --against <profile|uri>   # schema/count/bounds drift vs a second server; exit 1 = drift
sparrow profile <table> -o json            # per-column nulls / approx-distinct / min / max, one server-side pass
` + "```" + `

## Full command list

| command | what it does |
|---|---|
| ` + "`connect <uri>`" + ` | verify a server, save a profile |
| ` + "`orient`" + ` | one-shot markdown map: vendor, tables, schemas |
| ` + "`ls [pattern]`" + ` | list tables (pattern = SQL LIKE) |
| ` + "`info <table>`" + ` | schema, catalog, row count |
| ` + "`sql \"...\"`" + ` | run a statement (` + "`-`" + ` stdin, ` + "`-f`" + ` file, ` + "`--schema`" + `, ` + "`--stats`" + `, ` + "`--substrait`" + `) |
| ` + "`query <table>`" + ` | build a simple SELECT (` + "`--where --order --limit`" + `) |
| ` + "`head <table> [n]`" + ` | preview first n rows |
| ` + "`pull '<ticket>'`" + ` | Direct Pull (1-RTT); ` + "`doget`" + ` is a hidden alias |
| ` + "`profile <table>`" + ` | per-column null/distinct/min/max |
| ` + "`doctor [--server]`" + ` | connection diagnosis; ` + "`--server`" + ` = Flight SQL conformance card |
| ` + "`check <table>`" + ` | data-quality gate (exit 1 on findings) |
| ` + "`diff <t> --against`" + ` | drift gate vs a second server |
| ` + "`audit`" + ` | security surface of a server you operate |
| ` + "`ping`" + ` | latency percentiles: network vs server |
| ` + "`feedback \"...\"`" + ` | reach the sparrow maintainers |
| ` + "`profiles`" + ` | list / use / rm saved connections |
| ` + "`agent`" + ` | print this guide |

Connection flags work on every server command: ` + "`-s <profile|uri>`" + `,
` + "`--basic user:pass`" + `, ` + "`--bearer TOKEN`" + `, ` + "`--header k=v`" + `, ` + "`--tls-ca/--tls-cert/--tls-key`" + `.
Per-command help: ` + "`sparrow help <command>`" + ` or ` + "`sparrow <command> -h`" + `.

## A typical flow

` + "```sh" + `
sparrow connect grpc+tls://flight.sparrowflight.io:443 --basic demo:demo
sparrow orient                                              # what's here?
sparrow sql "SELECT * FROM search_meta('brent crude', lim := 5)" -o md   # find the series
sparrow pull '{"series":["PET.RWTC.D"]}' -o md              # pull a known one, 1 RTT
sparrow sql "SELECT period, value FROM series_data WHERE series_id='PET.RWTC.D'" -o data.parquet
` + "```" + `

## Found a bug or have an idea?

` + "`sparrow feedback \"...\" --from your-name`" + ` delivers it to the maintainers over
HTTPS, independent of whichever server you're connected to — so it works even
when the server is the problem. Agents are explicitly welcome to use it.

---
*Generated by ` + "`sparrow agent`" + `. For a specific server's live tables, schemas and
advertised macros, run ` + "`sparrow orient`" + ` (add ` + "`-s <profile>`" + ` to target one).*
`
