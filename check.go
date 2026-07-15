// check — the data doctor: a statistical health screen for one table,
// computed SERVER-SIDE with conservative SQL (COUNT / COUNT DISTINCT /
// MIN / MAX / AVG / STDDEV / GROUP BY ... HAVING — the subset that ran on
// every vendor we validated). The table itself is never downloaded.
//
// Checks: rows, null census, duplicate keys, time span + staleness,
// per-entity coverage, constant ("frozen") series, numeric ranges with a
// crude sigma-based outlier flag. Anything a dialect rejects degrades to
// "skip" with the server's error — one exotic server must not kill the
// whole checkup.
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/flight/flightsql"
)

// quoteIdent quotes a column name ANSI-style. Table names pass through
// tableExpr instead so dotted schema paths keep working.
func quoteIdent(c string) string {
	return `"` + strings.ReplaceAll(c, `"`, `""`) + `"`
}

// tableExpr: simple names get quoted; anything with a dot or quote is taken
// verbatim (the user is addressing a schema path in their server's dialect).
func tableExpr(t string) string {
	if strings.ContainsAny(t, `."`) {
		return t
	}
	return quoteIdent(t)
}

func isNumericType(id arrow.Type) bool {
	switch id {
	case arrow.INT8, arrow.INT16, arrow.INT32, arrow.INT64,
		arrow.UINT8, arrow.UINT16, arrow.UINT32, arrow.UINT64,
		arrow.FLOAT16, arrow.FLOAT32, arrow.FLOAT64,
		arrow.DECIMAL128, arrow.DECIMAL256:
		return true
	}
	return false
}

func isTemporalType(id arrow.Type) bool {
	switch id {
	case arrow.DATE32, arrow.DATE64, arrow.TIMESTAMP:
		return true
	}
	return false
}

// parseAge accepts Go durations plus a day suffix: "7d", "48h", "90m".
func parseAge(s string) (time.Duration, error) {
	if n, err := strconv.Atoi(strings.TrimSuffix(s, "d")); err == nil && strings.HasSuffix(s, "d") {
		return time.Duration(n) * 24 * time.Hour, nil
	}
	return time.ParseDuration(s)
}

var whenLayouts = []string{
	time.RFC3339,
	"2006-01-02 15:04:05Z07:00",
	"2006-01-02 15:04:05 -0700 MST",
	"2006-01-02 15:04:05",
	"2006-01-02T15:04:05",
	"2006-01-02",
	"2006-01",
	"2006",
}

func parseWhen(s string) (time.Time, bool) {
	s = strings.TrimSpace(s)
	for _, l := range whenLayouts {
		if t, err := time.Parse(l, s); err == nil {
			return t, true
		}
	}
	return time.Time{}, false
}

// queryRow runs a statement and returns the first row's cells (the stream is
// drained). nil row without error means the query returned no rows.
func queryRow(ctx context.Context, cl *flightsql.Client, sql string) ([]string, error) {
	info, err := cl.Execute(ctx, sql)
	if err != nil {
		return nil, err
	}
	var row []string
	for _, ep := range info.Endpoint {
		rdr, err := cl.DoGet(ctx, ep.Ticket)
		if err != nil {
			return nil, err
		}
		for rdr.Next() {
			rec := rdr.Record()
			if row == nil && rec.NumRows() > 0 {
				row = make([]string, rec.NumCols())
				for c := range row {
					row[c] = cell(rec.Column(c), 0)
				}
			}
		}
		err = rdr.Err()
		rdr.Release()
		if err != nil {
			return row, err
		}
	}
	return row, nil
}

// queryCol returns the first column of up to limit rows.
func queryCol(ctx context.Context, cl *flightsql.Client, sql string, limit int) ([]string, error) {
	info, err := cl.Execute(ctx, sql)
	if err != nil {
		return nil, err
	}
	var out []string
	for _, ep := range info.Endpoint {
		rdr, err := cl.DoGet(ctx, ep.Ticket)
		if err != nil {
			return out, err
		}
		for rdr.Next() {
			rec := rdr.Record()
			for r := 0; r < int(rec.NumRows()) && len(out) < limit; r++ {
				out = append(out, cell(rec.Column(0), r))
			}
		}
		err = rdr.Err()
		rdr.Release()
		if err != nil {
			return out, err
		}
	}
	return out, nil
}

// queryRows fetches up to limit rows, each as its full slice of column cells.
func queryRows(ctx context.Context, cl *flightsql.Client, sql string, limit int) ([][]string, error) {
	info, err := cl.Execute(ctx, sql)
	if err != nil {
		return nil, err
	}
	var out [][]string
	for _, ep := range info.Endpoint {
		rdr, err := cl.DoGet(ctx, ep.Ticket)
		if err != nil {
			return out, err
		}
		for rdr.Next() {
			rec := rdr.Record()
			for r := 0; r < int(rec.NumRows()) && len(out) < limit; r++ {
				cells := make([]string, rec.NumCols())
				for c := range cells {
					cells[c] = cell(rec.Column(c), r)
				}
				out = append(out, cells)
			}
		}
		err = rdr.Err()
		rdr.Release()
		if err != nil {
			return out, err
		}
	}
	return out, nil
}

func firstLine(err error) string {
	s := err.Error()
	if i := strings.IndexByte(s, '\n'); i > 0 {
		s = s[:i]
	}
	return s
}

func splitCols(s string) []string {
	var out []string
	for _, c := range strings.Split(s, ",") {
		if c = strings.TrimSpace(c); c != "" {
			out = append(out, c)
		}
	}
	return out
}

func cmdCheck(args []string) error {
	fs := newFlagSet("check", `usage: sparrow check <table> [flags]
data doctor: a server-side statistical health screen — rows, nulls, duplicate
keys, time span + staleness, coverage, frozen series, numeric ranges. The
table is never downloaded; every check is one conservative SQL aggregate.
examples: sparrow check series_data --key series_id --time period
          sparrow check trades --key "book,trade_id" --max-age 2d -o json`)
	cf := addConnFlags(fs)
	keyF := fs.String("key", "", "entity key column(s), comma-separated — enables duplicate/coverage/frozen checks")
	timeF := fs.String("time", "", "time column — enables span + staleness; with --key, uniqueness is checked on (key, time)")
	valueF := fs.String("value", "", "value column for the frozen-series check (default: the sole numeric column)")
	maxAgeF := fs.String("max-age", "", `warn when the newest time point is older than this ("7d", "48h")`)
	strict := fs.Bool("strict", false, "treat warnings as failures (exit 1) — for CI gates")
	showViol := fs.Bool("show-violations", false, "on a finding, emit sample offending keys + their conflicting values (up to 10), so you don't re-run the GROUP BY by hand")
	output := fs.String("o", "", `output: "json" for a machine-readable report`)
	pos := parseFlags(fs, args)
	if len(pos) < 1 {
		return usagef("usage: sparrow check <table> [--key cols] [--time col] [--max-age 7d] [-o json]")
	}
	table := pos[0]
	d := &doctor{}
	switch strings.ToLower(*output) {
	case "":
	case "json":
		d.json = true
	default:
		return usagef(`check -o supports only "json"`)
	}
	var maxAge time.Duration
	if *maxAgeF != "" {
		var err error
		if maxAge, err = parseAge(*maxAgeF); err != nil {
			return usagef("--max-age: %v", err)
		}
	}
	keys := splitCols(*keyF)

	p, pname, err := cf.resolve()
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	cl, ctx, err := dial(ctx, p)
	if err != nil {
		return err
	}
	defer cl.Close()

	t0 := time.Now()
	nq := 0
	texpr := tableExpr(table)
	row1 := func(sql string) ([]string, error) { nq++; return queryRow(ctx, cl, sql) }

	finish := func() error {
		d.rep.OK = d.fails == 0 && d.errs == 0
		if d.json {
			b, _ := json.MarshalIndent(d.rep, "", "  ")
			fmt.Println(string(b))
		} else {
			fmt.Println()
			line := fmt.Sprintf("%d ok · %d warn · %d fail", d.oks, d.warns, d.fails)
			if d.errs > 0 {
				line += fmt.Sprintf(" · %d error", d.errs)
			}
			fmt.Printf("%s — checked in %.1f s (%d queries, server-side)\n",
				line, time.Since(t0).Seconds(), nq)
		}
		if d.fails > 0 {
			return fmt.Errorf("check: %d finding(s) — see the ✗ lines", d.fails)
		}
		if d.errs > 0 {
			return fmt.Errorf("check: %d check(s) could not run — see the ! lines", d.errs)
		}
		if *strict && d.warns > 0 {
			return fmt.Errorf("check: %d warning(s) under --strict", d.warns)
		}
		return nil
	}

	// ── schema (LIMIT 0 probe — also proves the table is queryable) ───────
	nq++
	info, err := cl.Execute(ctx, "SELECT * FROM "+texpr+" LIMIT 0")
	var schema *arrow.Schema
	if err == nil {
		rdr, err2 := cl.DoGet(ctx, info.Endpoint[0].Ticket)
		if err2 == nil {
			schema = rdr.Schema()
			for rdr.Next() {
			}
			rdr.Release()
		} else {
			err = err2
		}
	}
	if err != nil || schema == nil {
		d.emit(checkResult{Check: "table", Status: "fail",
			Detail: fmt.Sprintf("%s is not queryable: %v", table, err),
			Hint:   "sparrow ls lists what the server exposes"})
		return finish()
	}
	d.rep.Endpoint, d.rep.Profile, d.rep.Table = p.URI, pname, table
	if !d.json {
		fmt.Printf("sparrow check — %s (profile: %s)\n\n", table, pname)
	}

	colType := map[string]arrow.Type{}
	for _, f := range schema.Fields() {
		colType[f.Name] = f.Type.ID()
	}
	for _, c := range append(append([]string{}, keys...), *timeF, *valueF) {
		if c != "" {
			if _, ok := colType[c]; !ok {
				d.emit(checkResult{Check: "table", Status: "fail",
					Detail: fmt.Sprintf("column %q not in %s (columns: %s)", c, table, colNames(schema))})
				return finish()
			}
		}
	}
	// auto-pick a temporal column when --time is not given
	timeCol := *timeF
	autoTime := ""
	if timeCol == "" {
		for _, f := range schema.Fields() {
			if isTemporalType(f.Type.ID()) {
				if autoTime != "" { // ambiguous — stay quiet
					autoTime = ""
					break
				}
				autoTime = f.Name
			}
		}
		timeCol = autoTime
	}
	tdesc := fmt.Sprintf("%d columns", schema.NumFields())
	if len(keys) > 0 {
		tdesc += " · key " + strings.Join(keys, ",")
	}
	if timeCol != "" {
		tdesc += " · time " + timeCol
		if autoTime != "" {
			tdesc += " (auto-detected)"
		}
	}
	d.emit(checkResult{Check: "table", Status: "ok", Detail: tdesc})

	// ── rows ──────────────────────────────────────────────────────────────
	var totalRows int64 = -1
	if row, err := row1("SELECT COUNT(*) FROM " + texpr); err != nil {
		d.emit(checkResult{Check: "rows", Status: "error", Detail: firstLine(err)})
	} else if row != nil {
		totalRows, _ = strconv.ParseInt(row[0], 10, 64)
		st := "ok"
		if totalRows == 0 {
			st = "fail"
		}
		d.emit(checkResult{Check: "rows", Status: st, Detail: groupDigits(row[0])})
		if totalRows == 0 {
			return finish()
		}
	}

	// ── null census (one query: COUNT(*) vs COUNT(col) per column) ────────
	cols := schema.Fields()
	if len(cols) > 100 {
		cols = cols[:100]
	}
	sel := "SELECT COUNT(*)"
	for _, f := range cols {
		sel += ", COUNT(" + quoteIdent(f.Name) + ")"
	}
	if row, err := row1(sel + " FROM " + texpr); err != nil {
		d.emit(checkResult{Check: "nulls", Status: "error", Detail: firstLine(err)})
	} else if row != nil {
		total, _ := strconv.ParseFloat(row[0], 64)
		st, parts, lines := "ok", []string{}, []string{}
		for i, f := range cols {
			nonNull, _ := strconv.ParseFloat(row[i+1], 64)
			pct := 100 * (1 - nonNull/total)
			if pct == 0 {
				continue
			}
			parts = append(parts, fmt.Sprintf("%s %.1f%%", f.Name, pct))
			if nonNull == 0 {
				st = "warn"
				lines = append(lines, f.Name+" is 100% NULL — a dead column")
			}
			for _, k := range keys {
				if f.Name == k {
					st = "fail"
					lines = append(lines, "key column "+k+" has NULLs — key integrity is broken")
				}
			}
			if f.Name == timeCol && st == "ok" {
				st = "warn"
				lines = append(lines, "time column "+timeCol+" has NULLs")
			}
		}
		detail := "no NULLs anywhere"
		if len(parts) > 0 {
			if len(parts) > 5 {
				parts = append(parts[:5], "…")
			}
			detail = strings.Join(parts, " · ")
		}
		d.emit(checkResult{Check: "nulls", Status: st, Detail: detail, Lines: lines})
	}

	// value column (hoisted: --show-violations on the keys check wants it too,
	// not just the frozen check below)
	valueCol := *valueF
	var numericCands []string
	for _, f := range schema.Fields() {
		if !isNumericType(f.Type.ID()) || f.Name == timeCol {
			continue
		}
		isKey := false
		for _, k := range keys {
			if f.Name == k {
				isKey = true
			}
		}
		if !isKey {
			numericCands = append(numericCands, f.Name)
		}
	}
	if valueCol == "" && len(numericCands) == 1 { // sole candidate: auto-pick
		valueCol = numericCands[0]
	}

	// ── duplicate keys: (key, time) when --time is set, else key ──────────
	if len(keys) == 0 {
		d.emit(checkResult{Check: "keys", Status: "skip", Detail: "pass --key <col[,col]> to check uniqueness"})
	} else {
		uniq := make([]string, len(keys))
		copy(uniq, keys)
		if timeCol != "" {
			uniq = append(uniq, timeCol)
		}
		qcols := make([]string, len(uniq))
		for i, c := range uniq {
			qcols[i] = quoteIdent(c)
		}
		gb := strings.Join(qcols, ", ")
		if row, err := row1("SELECT COUNT(*) FROM (SELECT " + gb + " FROM " + texpr +
			" GROUP BY " + gb + " HAVING COUNT(*) > 1) AS d"); err != nil {
			d.emit(checkResult{Check: "keys", Status: "error", Detail: firstLine(err)})
		} else if row != nil {
			n, _ := strconv.ParseInt(row[0], 10, 64)
			if n == 0 {
				d.emit(checkResult{Check: "keys", Status: "ok",
					Detail: "(" + strings.Join(uniq, ", ") + ") is unique"})
			} else {
				nq++
				res := checkResult{Check: "keys", Status: "fail",
					Detail: groupDigits(row[0]) + " duplicated (" + strings.Join(uniq, ", ") + ") groups"}
				if *showViol {
					// full offending tuples + their conflicting values, in one
					// server-side pass — the GROUP BY the caller would run by hand
					sel := gb + ", COUNT(*) AS n"
					if valueCol != "" {
						sel += ", STRING_AGG(DISTINCT CAST(" + quoteIdent(valueCol) +
							" AS VARCHAR), ' | ') AS vals"
					}
					rows, e := queryRows(ctx, cl, "SELECT "+sel+" FROM "+texpr+
						" GROUP BY "+gb+" HAVING COUNT(*) > 1 ORDER BY n DESC LIMIT 10", 10)
					if e == nil {
						for _, r := range rows {
							keyPart := strings.Join(r[:len(uniq)], ", ")
							line := keyPart + "  ×" + r[len(uniq)]
							if valueCol != "" && len(r) > len(uniq)+1 {
								line += "  " + valueCol + ": " + r[len(uniq)+1]
							}
							res.Lines = append(res.Lines, line)
						}
					}
				} else {
					ev, _ := queryCol(ctx, cl, "SELECT "+qcols[0]+" FROM "+texpr+
						" GROUP BY "+gb+" HAVING COUNT(*) > 1 LIMIT 3", 3)
					res.Lines = evLines("e.g. ", ev)
				}
				d.emit(res)
			}
		}
	}

	// ── time span + staleness ─────────────────────────────────────────────
	if timeCol == "" {
		st, hint := "skip", ""
		if maxAge > 0 {
			st = "warn"
			hint = "--max-age was given but no time column was found or auto-detected"
		}
		d.emit(checkResult{Check: "time", Status: st,
			Detail: "pass --time <col> for span + staleness", Hint: hint})
	} else {
		qt := quoteIdent(timeCol)
		if row, err := row1("SELECT MIN(" + qt + "), MAX(" + qt + ") FROM " + texpr); err != nil {
			d.emit(checkResult{Check: "time", Status: "error", Detail: firstLine(err)})
		} else if row != nil {
			lo, hi := row[0], row[1]
			st, hint := "ok", ""
			if lo == "" {
				lo = "(empty)"
				st = "warn"
				hint = "some " + timeCol + " values are empty strings"
			}
			if hi == "" {
				hi = "(empty)"
			}
			detail := fmt.Sprintf("%s spans %s → %s", timeCol, lo, hi)
			if newest, ok := parseWhen(hi); ok && row[1] != "" {
				age := time.Since(newest)
				detail += fmt.Sprintf(" · newest point %.1f days old", age.Hours()/24)
				if maxAge > 0 && age > maxAge {
					st = "warn"
					hint = fmt.Sprintf("older than --max-age %s — is the feed still running?", *maxAgeF)
				}
			} else if maxAge > 0 {
				st = "warn"
				hint = "can't parse the newest value as a time — staleness not computable"
			}
			d.emit(checkResult{Check: "time", Status: st, Detail: detail, Hint: hint})
		}
	}

	// ── per-entity coverage ───────────────────────────────────────────────
	if len(keys) > 0 {
		qcols := make([]string, len(keys))
		for i, c := range keys {
			qcols[i] = quoteIdent(c)
		}
		gb := strings.Join(qcols, ", ")
		if row, err := row1("SELECT COUNT(*), MIN(c), AVG(c), MAX(c) FROM (SELECT COUNT(*) AS c FROM " +
			texpr + " GROUP BY " + gb + ") AS s"); err != nil {
			d.emit(checkResult{Check: "coverage", Status: "error", Detail: firstLine(err)})
		} else if row != nil {
			avg, _ := strconv.ParseFloat(row[2], 64)
			d.emit(checkResult{Check: "coverage", Status: "ok",
				Detail: fmt.Sprintf("%s entities · rows per entity: min %s · avg %.0f · max %s",
					groupDigits(row[0]), groupDigits(row[1]), avg, groupDigits(row[3]))})
		}
	}

	// ── frozen series: entities whose value never changes ─────────────────
	// (valueCol + numericCands resolved above, before the keys check)
	if len(keys) > 0 && valueCol == "" {
		switch len(numericCands) {
		case 0:
			d.emit(checkResult{Check: "frozen", Status: "skip",
				Detail: "no numeric value column — nothing to test for constancy"})
		default:
			d.emit(checkResult{Check: "frozen", Status: "skip",
				Detail: "value column is ambiguous — pass --value (candidates: " +
					strings.Join(numericCands, ", ") + ")"})
		}
	}
	if len(keys) > 0 && valueCol != "" {
		qcols := make([]string, len(keys))
		for i, c := range keys {
			qcols[i] = quoteIdent(c)
		}
		gb := strings.Join(qcols, ", ")
		qv := quoteIdent(valueCol)
		// COUNT(col) not COUNT(*): ten real observations, all identical — null
		// rows must not help a series qualify
		having := " HAVING COUNT(DISTINCT " + qv + ") = 1 AND COUNT(" + qv + ") >= 10"
		if row, err := row1("SELECT COUNT(*) FROM (SELECT " + gb + " FROM " + texpr +
			" GROUP BY " + gb + having + ") AS f"); err != nil {
			d.emit(checkResult{Check: "frozen", Status: "error", Detail: firstLine(err)})
		} else if row != nil {
			n, _ := strconv.ParseInt(row[0], 10, 64)
			if n == 0 {
				d.emit(checkResult{Check: "frozen", Status: "ok",
					Detail: "no entity holds one constant " + valueCol + " over ≥10 observations"})
			} else {
				nq++
				res := checkResult{Check: "frozen", Status: "warn",
					Detail: groupDigits(row[0]) + " entities have a constant " + valueCol + " across ≥10 observations",
					Hint:   "constant series often mean a stuck feed or a fill-forward gone wrong"}
				if *showViol {
					rows, e := queryRows(ctx, cl, "SELECT "+gb+", ANY_VALUE("+qv+") AS v, COUNT("+qv+
						") AS n FROM "+texpr+" GROUP BY "+gb+having+" ORDER BY n DESC LIMIT 10", 10)
					if e == nil {
						for _, r := range rows {
							res.Lines = append(res.Lines, strings.Join(r[:len(keys)], ", ")+
								"  = "+r[len(keys)]+" ×"+r[len(keys)+1])
						}
					}
				} else {
					ev, _ := queryCol(ctx, cl, "SELECT "+qcols[0]+" FROM "+texpr+
						" GROUP BY "+gb+having+" LIMIT 3", 3)
					res.Lines = evLines("e.g. ", ev)
				}
				d.emit(res)
			}
		}
	}

	// ── numeric ranges + crude sigma outlier flag ─────────────────────────
	var numCols []string
	for _, f := range schema.Fields() {
		if isNumericType(f.Type.ID()) && f.Name != timeCol {
			numCols = append(numCols, f.Name)
		}
	}
	if len(numCols) > 8 {
		numCols = numCols[:8]
	}
	if len(numCols) == 0 {
		d.emit(checkResult{Check: "numeric", Status: "skip", Detail: "no numeric columns"})
	}
	if len(numCols) > 0 {
		sel := "SELECT"
		for i, c := range numCols {
			if i > 0 {
				sel += ","
			}
			q := quoteIdent(c)
			sel += fmt.Sprintf(" MIN(%s), MAX(%s), AVG(%s), STDDEV(%s)", q, q, q, q)
		}
		row, err := row1(sel + " FROM " + texpr)
		if err != nil { // STDDEV is the usual dialect casualty — retry without
			sel = "SELECT"
			for i, c := range numCols {
				if i > 0 {
					sel += ","
				}
				q := quoteIdent(c)
				sel += fmt.Sprintf(" MIN(%s), MAX(%s), AVG(%s), AVG(%s)", q, q, q, q)
			}
			row, err = row1(sel + " FROM " + texpr)
		}
		if err != nil {
			d.emit(checkResult{Check: "numeric", Status: "error", Detail: firstLine(err)})
		} else if row != nil {
			st, lines := "ok", []string{}
			var parts []string
			for i, c := range numCols {
				mn, mx, av := row[i*4], row[i*4+1], row[i*4+2]
				parts = append(parts, fmt.Sprintf("%s: min %s · max %s · avg %s", c, mn, mx, trimFloat(av)))
				sd, errSd := strconv.ParseFloat(row[i*4+3], 64)
				avf, _ := strconv.ParseFloat(av, 64)
				mxf, _ := strconv.ParseFloat(mx, 64)
				mnf, _ := strconv.ParseFloat(mn, 64)
				if errSd == nil && sd > 0 {
					if s := (mxf - avf) / sd; s > 6 {
						st = "warn"
						lines = append(lines, fmt.Sprintf("%s max is %.0fσ above the mean — outlier or unit error?", c, s))
					}
					if s := (avf - mnf) / sd; s > 6 {
						st = "warn"
						lines = append(lines, fmt.Sprintf("%s min is %.0fσ below the mean — outlier or unit error?", c, s))
					}
				}
			}
			d.emit(checkResult{Check: "numeric", Status: st, Detail: strings.Join(parts, " — "), Lines: lines})
		}
	}

	return finish()
}

func colNames(s *arrow.Schema) string {
	names := make([]string, s.NumFields())
	for i, f := range s.Fields() {
		names[i] = f.Name
	}
	return strings.Join(names, ", ")
}

func evLines(prefix string, vals []string) []string {
	if len(vals) == 0 {
		return nil
	}
	return []string{prefix + strings.Join(vals, ", ")}
}

func trimFloat(s string) string {
	if f, err := strconv.ParseFloat(s, 64); err == nil && !math.IsNaN(f) {
		return strconv.FormatFloat(f, 'f', 2, 64)
	}
	return s
}
