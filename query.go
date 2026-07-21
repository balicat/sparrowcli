// query — sugar over sql: build the one-liner SELECT for you.
// Agents write SQL; humans at 11pm write `sparrow query t --where "x>3"`.
package main

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
)

// withAcceptCompression augments a Sparrow-dialect JSON ticket with the codecs
// this client can decode, so a negotiating server ships a smaller wire (arrow-go
// decompresses lz4 transparently). Left untouched — safe — when: codecs is empty
// ("" / none / off), the ticket isn't a JSON object (bare id, other dialects),
// it already declares accept_compression, or it carries no Sparrow key
// (series/sql) so we don't perturb another vendor's dialect.
func withAcceptCompression(ticket []byte, codecs string) []byte {
	var list []string
	for _, c := range strings.Split(codecs, ",") {
		c = strings.ToLower(strings.TrimSpace(c))
		if c != "" && c != "none" && c != "off" {
			list = append(list, c)
		}
	}
	if len(list) == 0 {
		return ticket
	}
	var m map[string]json.RawMessage
	if json.Unmarshal(ticket, &m) != nil {
		return ticket // not a JSON object
	}
	if _, has := m["accept_compression"]; has {
		return ticket // caller set it explicitly
	}
	_, hasSeries := m["series"]
	_, hasSQL := m["sql"]
	if !hasSeries && !hasSQL {
		return ticket // not a Sparrow-dialect ticket — don't touch it
	}
	enc, err := json.Marshal(list)
	if err != nil {
		return ticket
	}
	m["accept_compression"] = enc
	out, err := json.Marshal(m)
	if err != nil {
		return ticket
	}
	return out
}

func cmdQuery(args []string) error {
	fs := newFlagSet("query", `usage: sparrow query <table> [flags]
build and run a simple SELECT without writing it: columns, WHERE, ORDER BY,
LIMIT — everything else (output formats, --stats, --ipc) works like sql
examples: sparrow query series_data --where "series_id='PET.RWTC.D'" --limit 20
          sparrow query trades --cols book,qty --order ts --desc --limit 50 -o md`)
	cf := addConnFlags(fs)
	cols := fs.String("cols", "*", "columns to select, comma-separated (expressions pass through)")
	where := fs.String("where", "", "WHERE clause, passed through verbatim")
	order := fs.String("order", "", "ORDER BY column or expression")
	desc := fs.Bool("desc", false, "ORDER BY … DESC")
	limit := fs.Int("limit", 0, "LIMIT n (0 = no limit)")
	maxRows := fs.Int("max-rows", -1, "max rows to emit (default: 40 table, 1000 md, unlimited otherwise)")
	output := fs.String("o", "", "output: table|csv|json|jsonl|md|arrow, or a file path (.parquet .csv …)")
	encKey := fs.String("encrypt-key", "", "encrypt parquet output: hex, env:VAR or file:path")
	statsOn := fs.Bool("stats", false, "print the query's anatomy to stderr")
	ipcOn := fs.Bool("ipc", false, "reveal the stream's IPC message manifest on stderr")
	bigintStr := fs.Bool("bigint-as-string", false, "emit int64/uint64 as quoted strings in json/jsonl")
	cost := fs.Bool("cost", false, "estimate result size (rows + bytes) and exit — nothing streamed")
	budgetSpec := fs.String("budget", "", `abort the stream past a ceiling: "10MB" | "5000rows" | "30s"; exit 1 on breach`)
	pos := parseFlags(fs, args)
	if len(pos) < 1 {
		return usagef(`usage: sparrow query <table> [--cols a,b] [--where "..."] [--order col] [--desc] [--limit N]`)
	}

	sel := make([]string, 0, 4)
	for _, c := range splitCols(*cols) {
		if c == "*" || strings.ContainsAny(c, "(*)+-/ ") {
			sel = append(sel, c) // expression or star: pass through
		} else {
			sel = append(sel, quoteIdent(c))
		}
	}
	q := "SELECT " + strings.Join(sel, ", ") + " FROM " + tableExpr(pos[0])
	if *where != "" {
		q += " WHERE " + *where
	}
	if *order != "" {
		o := *order
		if !strings.ContainsAny(o, "(*)+-/ ") {
			o = quoteIdent(o)
		}
		q += " ORDER BY " + o
		if *desc {
			q += " DESC"
		}
	}
	if *limit > 0 {
		q += " LIMIT " + strconv.Itoa(*limit)
	}
	if stdoutIsTTY() {
		fmt.Fprintln(os.Stderr, "sql: "+q)
	}
	xt := execExtra{cost: *cost}
	if *budgetSpec != "" {
		b, err := parseBudget(*budgetSpec)
		if err != nil {
			return usagef("%v", err)
		}
		xt.budget = &b
	}
	return execStatement(cf, q, nil, nil, *output, *encKey, *maxRows, *statsOn, *ipcOn, *bigintStr, xt)
}

// cmdHead — the SELECT * FROM t LIMIT n shortcut everyone types by hand.
func cmdHead(args []string) error {
	fs := newFlagSet("head", `usage: sparrow head <table> [n] [flags]
preview the first n rows (default 10) of a table — SELECT * FROM t LIMIT n.
examples: sparrow head series_data · sparrow head trades 20 -o md`)
	cf := addConnFlags(fs)
	output := fs.String("o", "", "output: table|csv|json|jsonl|md|arrow, or a file path")
	pos := parseFlags(fs, args)
	if len(pos) < 1 {
		return usagef("usage: sparrow head <table> [n]")
	}
	n := 10
	if len(pos) >= 2 {
		v, err := strconv.Atoi(pos[1])
		if err != nil || v < 1 {
			return usagef("head: row count must be a positive integer, got %q", pos[1])
		}
		n = v
	}
	q := "SELECT * FROM " + tableExpr(pos[0]) + " LIMIT " + strconv.Itoa(n)
	return execStatement(cf, q, nil, nil, *output, "", n, false, false, false, execExtra{})
}

// cmdPull — a Direct Pull (1-RTT): a ready ticket straight to DoGet, no
// GetFlightInfo, no SQL. Flight SQL reads are two round trips by design
// (query → ticket → stream); servers that accept client-constructed tickets
// serve known pulls in ONE. Measured on the public demo: 143 ms vs 224 ms
// for the same 10k-row series. Invoked as `sparrow pull`; `doget` (the DoGet
// RPC name) is kept as a hidden alias.
func cmdPull(args []string) error {
	fs := newFlagSet("pull", `usage: sparrow pull '<ticket>' [flags]
Direct Pull (1-RTT): send a ready ticket STRAIGHT to the server (DoGet),
skipping GetFlightInfo and SQL entirely. Ticket dialects are server-specific;
Sparrow serving nodes accept JSON: {"series": ["ID", ...], "start": "...",
"end": "..."} — or {"sql": "SELECT …"} for an arbitrary read-only query.
Servers that mint opaque statement handles (GizmoSQL, DataFusion) reject
ready tickets — use sparrow sql there. doctor --server probes which kind a
server is ("direct JSON tickets").
By default a Sparrow ticket also requests lz4 compression (the server
compresses only if it offers it; arrow-go decodes transparently and --stats
reports the codec + ratio). Pass --accept-compression "" to send verbatim.
--dry-run prints the FINAL composed ticket (after any accept_compression
injection) to stdout and exits without connecting — see exactly what would go
on the wire.
examples: sparrow pull '{"series": ["PET.RWTC.D"]}'
          sparrow pull '{"series": ["FRED.DFF"], "start": "2020-01-01"}' -o md
          sparrow pull @ticket.json -o data.parquet
          sparrow pull '{"sql": "SELECT …"}' --stats   # wire line shows codec lz4_frame
          sparrow pull '{"series": ["PET.RWTC.D"]}' --dry-run   # show the wire ticket, send nothing
          echo '{"series": ["PET.RWTC.D"]}' | sparrow pull - --stats`)
	cf := addConnFlags(fs)
	output := fs.String("o", "", "output: table|csv|json|jsonl|md|arrow, or a file path")
	encKey := fs.String("encrypt-key", "", "seal parquet output: hex key, env:VAR, or file:path")
	maxRows := fs.Int("max-rows", 0, "cap rows fetched (0 = format default)")
	statsOn := fs.Bool("stats", false, "print stream anatomy to stderr")
	ipcOn := fs.Bool("ipc", false, "print the raw IPC manifest to stderr")
	bigintStr := fs.Bool("bigint-as-string", false, "emit int64 as quoted strings in JSON output")
	acceptComp := fs.String("accept-compression", "lz4",
		"codecs to request on a Sparrow ticket (comma list); the server compresses only for a listed one and arrow-go decodes it transparently. \"\"|none to send the ticket verbatim")
	dryRun := fs.Bool("dry-run", false, "print the final composed ticket (after accept_compression injection) to stdout and exit — nothing is sent")
	budgetSpec := fs.String("budget", "", `abort the stream past a ceiling: "10MB" | "5000rows" | "30s"; exit 1 on breach — a safety net when pulling an unknown series`)
	pos := parseFlags(fs, args)
	if len(pos) < 1 {
		return usagef("usage: sparrow pull '<ticket>' (or @file, or - for stdin)")
	}
	raw := pos[0]
	var ticket []byte
	switch {
	case raw == "-":
		b, err := io.ReadAll(os.Stdin)
		if err != nil {
			return err
		}
		ticket = []byte(strings.TrimSpace(string(b)))
	case strings.HasPrefix(raw, "@"):
		b, err := os.ReadFile(raw[1:])
		if err != nil {
			return err
		}
		ticket = []byte(strings.TrimSpace(string(b)))
	default:
		ticket = []byte(raw)
	}
	if len(ticket) == 0 {
		return usagef("pull: empty ticket")
	}
	ticket = withAcceptCompression(ticket, *acceptComp)
	if *dryRun {
		// Tester wish #1 (2026-07-20): the injection was only verifiable
		// behaviorally (or with a wire proxy) — print the EXACT bytes DoGet
		// would carry, so the inject/negotiate path is self-verifying.
		fmt.Println(string(ticket))
		return nil
	}
	xt := execExtra{}
	if *budgetSpec != "" {
		b, err := parseBudget(*budgetSpec)
		if err != nil {
			return usagef("%v", err)
		}
		xt.budget = &b
	}
	return execStatement(cf, "", nil, ticket, *output, *encKey, *maxRows, *statsOn, *ipcOn, *bigintStr, xt)
}
