// agent — `sparrow agent` prints a complete, self-contained operator's manual
// for an AI agent (Claude Code et al.) driving the CLI. One command, one
// markdown document, no server required: everything an agent needs to discover
// a Flight server, read data the fast way, parse the output, and recover from
// errors. Point an agent at `sparrow agent` once and it can operate the tool.
package main

import (
	"encoding/json"
	"fmt"
	"sort"
	"strings"
)

func cmdAgent(args []string) error {
	fs := newFlagSet("agent", `usage: sparrow agent [--json]
Print a complete agent-ready manual (markdown) for driving sparrow, to stdout.
Self-contained — no server connection needed. Save it or pipe it:
  sparrow agent > SPARROW.md
--json emits a machine-readable capability catalog instead (commands, flags,
exit codes, ticket dialects, output formats) — for programmatic bootstrap
against an unknown sparrow version.
For a specific server's live tables and macros, run `+"`sparrow orient`"+` after.`)
	asJSON := fs.Bool("json", false, "emit a machine-readable capability catalog (JSON) instead of the markdown manual")
	parseFlags(fs, args)
	if *asJSON {
		return agentJSON()
	}
	fmt.Print(strings.Replace(agentGuide, "{{VERSION}}", versionString(), 1))
	return nil
}

// agentJSON — tester wish #3 (2026-07-20): a structured self-description an
// agent can parse instead of scraping markdown. Built from the SAME tables
// shell completion uses (completion.go), so it cannot drift from the real
// command/flag surface.
func agentJSON() error {
	type cmdEntry struct {
		Name        string   `json:"name"`
		Description string   `json:"description"`
		Flags       []string `json:"flags"`
		ServerSide  bool     `json:"server_side"` // accepts connection flags
	}
	names := make([]string, 0, len(cmdDesc))
	for c := range cmdDesc {
		names = append(names, c)
	}
	sort.Strings(names)
	cmds := make([]cmdEntry, 0, len(names))
	for _, c := range names {
		flags := make([]string, 0, 8)
		for _, f := range cmdOwnFlags[c] {
			flags = append(flags, "--"+f)
		}
		cmds = append(cmds, cmdEntry{
			Name: c, Description: cmdDesc[c], Flags: flags, ServerSide: serverCmds[c],
		})
	}
	connFlags := make([]string, 0, len(connFlagNames))
	for _, f := range connFlagNames {
		connFlags = append(connFlags, "--"+f)
	}
	cat := map[string]any{
		"name":        "sparrow",
		"version":     versionString(),
		"description": "command-line client for any Apache Arrow Flight / Flight SQL server",
		"exit_codes": map[string]string{
			"0": "ok",
			"1": "query error, or a gate hit (check findings, diff drift, audit exposure)",
			"2": "connection/auth failure — run `sparrow doctor -o json` for a layered diagnosis",
			"3": "usage error",
		},
		"connection_flags": connFlags,
		"commands":         cmds,
		"output_formats":   []string{"table", "csv", "json", "jsonl", "md", "arrow", "<file path: .parquet .csv .json .jsonl .arrow .md>"},
		"output_conventions": map[string]any{
			"stdout":              "data only (md/jsonl/csv are stable and ANSI-free; a pipe defaults to raw Arrow IPC)",
			"stderr":              "row-count and timing summaries, --stats/--ipc anatomy",
			"md_stdout_row_cap":   1000,
			"row_cap_override":    "--max-rows",
			"schema_only_no_rows": "sql --schema",
		},
		"ticket_dialects": map[string]any{
			"series":      map[string]any{"example": map[string]any{"series": []string{"ID1", "ID2"}, "start": "2020-01-01", "end": "2021-01-01"}, "note": "start/end optional; unknown ids are omitted, not errors"},
			"sql":         map[string]any{"example": map[string]any{"sql": "SELECT ..."}, "note": "read-only; server enforces"},
			"negotiation": "pull injects accept_compression (default lz4) into Sparrow-dialect tickets; --dry-run shows the final ticket without sending",
		},
		"docs": map[string]string{
			"manual":           "sparrow agent            (markdown, self-contained)",
			"per_command_help": "sparrow help <command>   (or <command> -h)",
			"live_catalog":     "sparrow orient           (server-specific tables, schemas, macros)",
		},
	}
	out, err := json.MarshalIndent(cat, "", "  ")
	if err != nil {
		return err
	}
	fmt.Println(string(out))
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

**Reusable tickets.** A ticket is a plain, stateless artifact — the server
re-runs it fresh every ` + "`pull`" + `, so you can save one and replay it forever (it
even survives a server restart; a GetFlightInfo statement handle does NOT — it's
single-use). ` + "`sparrow ticket`" + ` writes one for you, JSON-escaped:
` + "```sh" + `
sparrow ticket "SELECT period, value FROM series_data WHERE series_id='PET.RWTC.D'" > wti.ticket
sparrow pull @wti.ticket -o md          # replay it, as often as you like — 1 RTT each
` + "```" + `
(` + "`sparrow ticket --series A,B [--start ..]`" + ` builds a series ticket instead.)

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

## Don't flood yourself — size awareness (estimate → decide → enforce)

Before pulling something you can't size, **estimate**; when you can't trust the
size, **enforce a ceiling**. Two commands, both self-preservation:

- ` + "`sparrow sql \"...\" --cost`" + ` — prints an estimate to stderr and streams
  **nothing**: exact row count (` + "`count(*)`" + `) + a decoded-bytes extrapolation from
  a first-batch sample + whether it exceeds the 1000-row ` + "`-o md`" + ` cap. The "how
  much" sibling of ` + "`--schema`" + `'s "what shape".
- ` + "`--budget \"10MB\" | \"5000rows\" | \"30s\"`" + ` (on ` + "`sql`" + `/` + "`query`" + `/` + "`pull`" + `) — a hard
  ceiling. The stream is **aborted** the moment it's crossed, **exit 1**. Rows
  cap exactly (you get all N); bytes/time stop at a batch boundary. Comma-AND
  multiple: ` + "`--budget 10MB,30s`" + `. A budget-stopped result is a FAILURE, not a
  short answer — never treat exit 1 here as "the whole result".

` + "```sh" + `
sparrow sql "SELECT * FROM series_data WHERE series_id='PET.RWTC.D'" --cost   # ~10,217 rows / ~347 KB — decide before fetching
sparrow pull '{"series":["UNKNOWN"]}' --budget 5MB -o data.parquet            # safety net on an unknown series
` + "```" + `

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

## Prove a number — verifiable receipts

When you report a figure from the data, back it with a **receipt** so anyone can
confirm it wasn't invented:

- ` + "`sparrow sql \"...\" --receipt r.json`" + ` runs the query normally AND writes a
  manifest: the query, the server's identity, a timestamp, and an
  **order-independent content fingerprint** of the result (count + two digests,
  computed server-side — nothing extra downloaded).
- ` + "`sparrow verify r.json`" + ` re-runs the query and confirms it: **exit 0** =
  verified, **exit 1** = the result, its column shape, or the server's identity
  no longer matches, **exit 2** = couldn't reach/authenticate, **exit 3** = the
  receipt itself is malformed. Against the receipt's own endpoint it also checks
  the server still identifies the same; ` + "`-s <other>`" + ` deliberately targets a
  different server to ask "do two engines agree on the DATA?".

` + "```sh" + `
sparrow sql "SELECT count(*) FROM series_data WHERE value < 0" -o md --receipt neg.json
sparrow verify neg.json          # ✓ verified — the count is real
` + "```" + `

Compose it with ` + "`expect`" + `: **assert** a contract, **receipt** the result,
let a third party **verify** — your analysis is checkable, not just plausible.
(A non-deterministic query — ` + "`now()`" + `, ` + "`random()`" + ` — won't verify; that's correct.)

## Reproducible investigations — record & replay

To make a whole exploration replayable, set ` + "`SPARROW_SESSION=<file>`" + ` and every
read appends a step (query + endpoint + row count + a fingerprint for SQL):

` + "```sh" + `
SPARROW_SESSION=probe.jsonl sparrow sql "SELECT count(*) FROM series_data WHERE value<0"
SPARROW_SESSION=probe.jsonl sparrow sql "SELECT max(value) FROM series_data"
sparrow replay probe.jsonl          # …does the whole investigation still hold?
` + "```" + `

` + "`sparrow replay`" + ` re-runs each SQL step and diffs its fingerprint — **exit 0** if
the investigation reproduces, **exit 1** if any step drifted (it names which and
how) — or if the file has no verifiable steps at all (nothing confirmed ≠
confirmed). ` + "`replay -s <other>`" + ` runs the whole investigation against a different
server. "Here's how I got this number" becomes a regression test.
Caveat: the SQL text is recorded verbatim (replay must re-run it) — credential
FLAGS are redacted, but a secret inside a query literal is not. Keep secrets
out of SQL you record.

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
sparrow check <table> --fail-on keys       # gate the exit on named checks only; the rest still report
sparrow expect "<sql>" --eq N              # assert a query result (--eq/--rows/--empty/--cols); exit 1 on violation — pin a finding as a contract
sparrow audit -o json                      # what client SQL reaches beyond queries (files, SSRF, catalog writes) — a server you operate
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
| ` + "`sql \"...\"`" + ` | run a statement (` + "`-`" + ` stdin, ` + "`-f`" + ` file, ` + "`--schema`" + `, ` + "`--cost`" + `, ` + "`--budget`" + `, ` + "`--stats`" + `, ` + "`--substrait`" + `) |
| ` + "`query <table>`" + ` | build a simple SELECT (` + "`--where --order --limit`" + `) |
| ` + "`head <table> [n]`" + ` | preview first n rows |
| ` + "`pull '<ticket>'`" + ` | Direct Pull (1-RTT); ` + "`--budget`" + ` caps the stream; ` + "`doget`" + ` is a hidden alias |
| ` + "`ticket \"<sql>\"`" + ` | emit a reusable pull ticket (JSON) to save & replay |
| ` + "`profile <table>`" + ` | per-column null/distinct/min/max |
| ` + "`doctor [--server]`" + ` | connection diagnosis; ` + "`--server`" + ` = Flight SQL conformance card |
| ` + "`check <table>`" + ` | data-quality gate (exit 1 on findings) |
| ` + "`expect \"<sql>\"`" + ` | assert a query result (--eq/--rows/--empty/--cols); exit 1 on violation |
| ` + "`verify <receipt>`" + ` | re-run a receipt's query, confirm the fingerprint matches (` + "`sql --receipt`" + ` writes one) |
| ` + "`replay <session>`" + ` | re-run a recorded investigation (SPARROW_SESSION=file), confirm every step reproduces |
| ` + "`mcp [-s profile]`" + ` | serve orient/sql/pull/expect/verify + doctor/check/ping + version/whatsnew/feedback over MCP stdio — for hosts WITHOUT a shell (Claude Desktop etc.); this manual is for shell agents, same verbs there |
| ` + "`whatsnew [-n N]`" + ` | recent release notes, live from the releases feed |
| ` + "`diff <t> --against`" + ` | drift gate vs a second server |
| ` + "`audit`" + ` | security surface of a server you operate |
| ` + "`ping`" + ` | latency percentiles: network vs server |
| ` + "`feedback \"...\"`" + ` | reach the sparrow maintainers |
| ` + "`profiles`" + ` | list / use / rm saved connections |
| ` + "`agent [--json]`" + ` | print this guide; ` + "`--json`" + ` = a parseable capability catalog |

Connection flags work on every server command: ` + "`-s <profile|uri>`" + `,
` + "`--basic user:pass`" + `, ` + "`--bearer TOKEN`" + `, ` + "`--header k=v`" + `, ` + "`--tls-ca/--tls-cert/--tls-key`" + `.
(` + "`ticket`" + `, ` + "`agent`" + `, ` + "`profiles`" + ` and ` + "`completion`" + ` are client-side only — they
take NO connection flags; passing ` + "`-s`" + ` there is a usage error.)
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
