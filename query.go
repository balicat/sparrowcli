// query — sugar over sql: build the one-liner SELECT for you.
// Agents write SQL; humans at 11pm write `sparrow query t --where "x>3"`.
package main

import (
	"fmt"
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
	return execStatement(cf, q, nil, *output, *encKey, *maxRows, *statsOn, *ipcOn)
}
