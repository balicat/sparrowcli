// query — sugar over sql: build the one-liner SELECT for you.
// Agents write SQL; humans at 11pm write `sparrow query t --where "x>3"`.
package main

import (
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
)

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
	return execStatement(cf, q, nil, nil, *output, *encKey, *maxRows, *statsOn, *ipcOn, *bigintStr)
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
	return execStatement(cf, q, nil, nil, *output, "", n, false, false, false)
}

// cmdDoGet — the 1-RTT pull: a raw ticket straight to DoGet, no
// GetFlightInfo, no SQL. Flight SQL reads are two round trips by design
// (query → ticket → stream); servers that accept client-constructed tickets
// serve known pulls in ONE. Measured on the public demo: 143 ms vs 224 ms
// for the same 10k-row series.
func cmdDoGet(args []string) error {
	fs := newFlagSet("doget", `usage: sparrow doget '<ticket>' [flags]
1-RTT pull: send a raw ticket STRAIGHT to DoGet — skipping GetFlightInfo
(and SQL entirely). Ticket dialects are server-specific; Sparrow serving
nodes accept JSON: {"series": ["ID", ...], "start": "...", "end": "..."}.
Servers that mint opaque statement handles (GizmoSQL, DataFusion) reject
raw tickets — use sparrow sql there. doctor --server probes which kind a
server is ("direct JSON tickets").
examples: sparrow doget '{"series": ["PET.RWTC.D"]}'
          sparrow doget '{"series": ["FRED.DFF"], "start": "2020-01-01"}' -o md
          sparrow doget @ticket.json -o data.parquet
          echo '{"series": ["PET.RWTC.D"]}' | sparrow doget - --stats`)
	cf := addConnFlags(fs)
	output := fs.String("o", "", "output: table|csv|json|jsonl|md|arrow, or a file path")
	encKey := fs.String("encrypt-key", "", "seal parquet output: hex key, env:VAR, or file:path")
	maxRows := fs.Int("max-rows", 0, "cap rows fetched (0 = format default)")
	statsOn := fs.Bool("stats", false, "print stream anatomy to stderr")
	ipcOn := fs.Bool("ipc", false, "print the raw IPC manifest to stderr")
	bigintStr := fs.Bool("bigint-as-string", false, "emit int64 as quoted strings in JSON output")
	pos := parseFlags(fs, args)
	if len(pos) < 1 {
		return usagef("usage: sparrow doget '<ticket>' (or @file, or - for stdin)")
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
		return usagef("doget: empty ticket")
	}
	return execStatement(cf, "", nil, ticket, *output, *encKey, *maxRows, *statsOn, *ipcOn, *bigintStr)
}
