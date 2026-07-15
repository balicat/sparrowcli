// profile — a one-pass column profiler: per-column null %, distinct estimate,
// min and max, computed SERVER-SIDE in a single query. `check` screens a
// table for problems; `profile` describes its columns' distributions.
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
)

func cmdProfile(args []string) error {
	fs := newFlagSet("profile", `usage: sparrow profile <table> [flags]
per-column profile — nulls, approx distinct count, min, max — for every
column, in ONE server-side pass. The table is never downloaded.
examples: sparrow profile series_data · sparrow profile trades -o json`)
	cf := addConnFlags(fs)
	output := fs.String("o", "", `output: "json" for a machine-readable report`)
	pos := parseFlags(fs, args)
	if len(pos) < 1 {
		return usagef("usage: sparrow profile <table> [-o json]")
	}
	jsonOut := false
	switch strings.ToLower(*output) {
	case "":
	case "json":
		jsonOut = true
	default:
		return usagef(`profile -o supports only "json"`)
	}
	table := pos[0]
	texpr := tableExpr(table)

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

	// schema via a LIMIT 0 probe
	info, err := cl.Execute(ctx, "SELECT * FROM "+texpr+" LIMIT 0")
	if err != nil {
		return err
	}
	rdr, err := cl.DoGet(ctx, info.Endpoint[0].Ticket)
	if err != nil {
		return err
	}
	schema := rdr.Schema()
	for rdr.Next() {
	}
	rdr.Release()
	if schema == nil || schema.NumFields() == 0 {
		return fmt.Errorf("profile: %s has no columns", table)
	}

	// one pass: total rows + per-column non-null, approx distinct, min, max.
	// min/max only for numeric/temporal/string columns (skip nested types).
	type colspec struct {
		name, typ  string
		profilable bool
	}
	var cols []colspec
	sel := []string{"COUNT(*) AS __total"}
	for _, f := range schema.Fields() {
		q := quoteIdent(f.Name)
		id := strconv.Itoa(len(cols))
		prof := isNumericType(f.Type.ID()) || isTemporalType(f.Type.ID()) ||
			f.Type.ID() == arrow.STRING || f.Type.ID() == arrow.LARGE_STRING
		cols = append(cols, colspec{f.Name, f.Type.String(), prof})
		sel = append(sel,
			"COUNT("+q+") AS nn_"+id,
			"approx_count_distinct("+q+") AS dc_"+id)
		if prof {
			sel = append(sel,
				"CAST(MIN("+q+") AS VARCHAR) AS mn_"+id,
				"CAST(MAX("+q+") AS VARCHAR) AS mx_"+id)
		}
	}

	t0 := time.Now()
	row, err := queryRow(ctx, cl, "SELECT "+strings.Join(sel, ", ")+" FROM "+texpr)
	if err != nil {
		return err
	}
	if row == nil {
		return fmt.Errorf("profile: %s returned no aggregate row", table)
	}

	// the columns come back positionally in the order we built them
	total, _ := strconv.ParseInt(row[0], 10, 64)
	type colProfile struct {
		Column   string  `json:"column"`
		Type     string  `json:"type"`
		Nulls    int64   `json:"nulls"`
		NullPct  float64 `json:"null_pct"`
		Distinct int64   `json:"distinct_approx"`
		Min      string  `json:"min,omitempty"`
		Max      string  `json:"max,omitempty"`
	}
	var profiles []colProfile
	idx := 1
	for _, c := range cols {
		nn, _ := strconv.ParseInt(row[idx], 10, 64)
		dc, _ := strconv.ParseInt(row[idx+1], 10, 64)
		idx += 2
		if dc > total { // approx_count_distinct (HLL) can overshoot the row count
			dc = total
		}
		cp := colProfile{Column: c.name, Type: c.typ, Nulls: total - nn, Distinct: dc}
		if total > 0 {
			cp.NullPct = 100 * float64(total-nn) / float64(total)
		}
		if c.profilable {
			cp.Min, cp.Max = row[idx], row[idx+1]
			idx += 2
		}
		profiles = append(profiles, cp)
	}

	if jsonOut {
		out := struct {
			Endpoint string       `json:"endpoint"`
			Table    string       `json:"table"`
			Rows     int64        `json:"rows"`
			Columns  []colProfile `json:"columns"`
		}{p.URI, table, total, profiles}
		b, _ := json.MarshalIndent(out, "", "  ")
		fmt.Println(string(b))
		return nil
	}

	fmt.Printf("# %s — %s rows (profile: %s)\n\n", table, groupDigits(row[0]), pname)
	tw := tabwriter.NewWriter(os.Stdout, 2, 4, 2, ' ', 0)
	fmt.Fprintln(tw, "column\ttype\tnulls\tdistinct≈\tmin\tmax")
	for _, c := range profiles {
		nulls := groupDigits(fmt.Sprint(c.Nulls))
		if c.Nulls > 0 {
			nulls += fmt.Sprintf(" (%.0f%%)", c.NullPct)
		}
		fmt.Fprintf(tw, "%s\t%s\t%s\t%s\t%s\t%s\n", c.Column, c.Type, nulls,
			groupDigits(fmt.Sprint(c.Distinct)), trunc(oneLine(c.Min), 24), trunc(oneLine(c.Max), 24))
	}
	tw.Flush()
	fmt.Printf("\nprofiled in %.1f s (1 query, server-side)\n", time.Since(t0).Seconds())
	return nil
}

func trunc(s string, n int) string {
	if len(s) > n {
		return s[:n-1] + "…"
	}
	return s
}

// oneLine collapses newlines/tabs so a MIN/MAX string value can't break the
// profile table's column alignment.
func oneLine(s string) string {
	return strings.TrimSpace(strings.NewReplacer("\n", " ", "\r", " ", "\t", " ").Replace(s))
}
