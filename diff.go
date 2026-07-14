// diff — compare one table across two servers (the drift gate).
//
// Made for the "is the replica current?" question: schema, row count, time
// bounds, and a numeric fingerprint, side by side. Identical → exit 0;
// any difference → exit 1, so a cron line can gate on it. The B side is a
// profile name or an ANONYMOUS URI — for an authenticated ad-hoc B, save a
// profile first (sparrow connect ... --name b).
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
	"github.com/apache/arrow-go/v18/arrow/flight"
	"github.com/apache/arrow-go/v18/arrow/flight/flightsql"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

type diffCheck struct {
	Check string `json:"check"`
	A     string `json:"a"`
	B     string `json:"b"`
	Same  bool   `json:"same"`
}

type diffReport struct {
	Table  string      `json:"table"`
	A      string      `json:"a"`
	B      string      `json:"b"`
	Checks []diffCheck `json:"checks"`
	Same   bool        `json:"same"`
}

func cmdDiff(args []string) error {
	fs := newFlagSet("diff", `usage: sparrow diff <table> --against <profile-or-uri> [flags]
compare the table on two servers: schema, COUNT(*), --time MIN/MAX bounds,
and a numeric fingerprint (COUNT+AVG of up to 4 shared numeric columns).
identical → exit 0 · any drift → exit 1, so cron can gate on it
examples: sparrow diff series_data --against gizmo --time period
          sparrow diff trades --against grpc+tls://replica:443 -o json`)
	cf := addConnFlags(fs)
	against := fs.String("against", "", "B side: profile name or anonymous grpc URI (required)")
	timeCol := fs.String("time", "", "temporal column: compare MIN/MAX bounds")
	output := fs.String("o", "", `output: "json" for a machine-readable report`)
	pos := parseFlags(fs, args)
	if len(pos) != 1 || *against == "" {
		return usagef("usage: sparrow diff <table> --against <profile-or-uri> [--time col] [-o json]")
	}
	jsonOut := false
	switch strings.ToLower(*output) {
	case "":
	case "json":
		jsonOut = true
	default:
		return usagef(`diff -o supports only "json"`)
	}
	table := pos[0]
	texpr := tableExpr(table)

	pa, aname, err := cf.resolve()
	if err != nil {
		return err
	}
	pb, bname, err := resolveProfile(*against, Profile{Auth: "none"})
	if err != nil {
		return err
	}

	// two independent dials — each carries its OWN auth metadata context
	base, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	ca, ctxA, err := dial(base, pa)
	if err != nil {
		return connError{fmt.Errorf("A (%s): %w", aname, err)}
	}
	defer ca.Close()
	cb, ctxB, err := dial(base, pb)
	if err != nil {
		return connError{fmt.Errorf("B (%s): %w", bname, err)}
	}
	defer cb.Close()

	rep := diffReport{Table: table,
		A: fmt.Sprintf("%s (%s)", pa.URI, aname),
		B: fmt.Sprintf("%s (%s)", pb.URI, bname)}
	if !jsonOut {
		fmt.Printf("sparrow diff %s\nA: %-10s %s\nB: %-10s %s\n\n", table, aname, pa.URI, bname, pb.URI)
	}
	emit := func(c diffCheck) {
		rep.Checks = append(rep.Checks, c)
		if jsonOut {
			return
		}
		if c.Same {
			fmt.Printf(" ✓ %-12s %s\n", c.Check, c.A)
		} else {
			fmt.Printf(" ✗ %-12s A %s · B %s\n", c.Check, c.A, c.B)
		}
	}

	// ── schema (LIMIT 0 both sides) ─────────────────────────────────────
	sa, errA := diffSchema(ctxA, ca, texpr)
	sb, errB := diffSchema(ctxB, cb, texpr)
	if errA != nil || errB != nil {
		side, name, err := "A", aname, errA
		if errA == nil {
			side, name, err = "B", bname, errB
		}
		return fmt.Errorf("diff: %s (%s): table %s is not queryable: %v", side, name, table, err)
	}
	amap, bmap := fieldTypes(sa), fieldTypes(sb)
	var added, removed, retyped []string
	for _, f := range sb.Fields() {
		if _, ok := amap[f.Name]; !ok {
			added = append(added, f.Name)
		}
	}
	for _, f := range sa.Fields() {
		bt, ok := bmap[f.Name]
		if !ok {
			removed = append(removed, f.Name)
		} else if at := amap[f.Name]; at != bt {
			retyped = append(retyped, fmt.Sprintf("%s: %s→%s", f.Name, at, bt))
		}
	}
	if len(added)+len(removed)+len(retyped) == 0 {
		emit(diffCheck{Check: "schema", Same: true,
			A: fmt.Sprintf("%d columns, identical", sa.NumFields()), B: "same"})
	} else {
		var parts []string
		if len(added) > 0 {
			parts = append(parts, "adds "+strings.Join(added, ","))
		}
		if len(removed) > 0 {
			parts = append(parts, "drops "+strings.Join(removed, ","))
		}
		if len(retyped) > 0 {
			parts = append(parts, "retypes "+strings.Join(retyped, ", "))
		}
		emit(diffCheck{Check: "schema", Same: false,
			A: fmt.Sprintf("%d columns", sa.NumFields()), B: strings.Join(parts, " · ")})
	}

	// ── row count ───────────────────────────────────────────────────────
	compare := func(name, sql string, render func(row []string) string, numeric bool) {
		ra, errA := queryRow(ctxA, ca, sql)
		rb, errB := queryRow(ctxB, cb, sql)
		switch {
		case errA != nil:
			emit(diffCheck{Check: name, Same: false, A: "error: " + firstLine(errA), B: renderOr(rb, render)})
		case errB != nil:
			emit(diffCheck{Check: name, Same: false, A: renderOr(ra, render), B: "error: " + firstLine(errB)})
		default:
			same := rowsEqual(ra, rb, numeric)
			c := diffCheck{Check: name, Same: same, A: render(ra), B: render(rb)}
			if same {
				c.B = "same"
			}
			emit(c)
		}
	}

	compare("rows", "SELECT COUNT(*) FROM "+texpr, func(r []string) string {
		if len(r) > 0 {
			return groupDigits(r[0])
		}
		return "(empty)"
	}, false)

	// ── time bounds (--time) ────────────────────────────────────────────
	if *timeCol != "" {
		tc := quoteIdent(*timeCol)
		compare("time", fmt.Sprintf("SELECT MIN(%s), MAX(%s) FROM %s", tc, tc, texpr),
			func(r []string) string {
				if len(r) >= 2 {
					return r[0] + " → " + r[1]
				}
				return "(empty)"
			}, false)
	}

	// ── numeric fingerprint: COUNT+AVG of ≤4 shared numeric columns ─────
	var fpCols []string
	for _, f := range sa.Fields() {
		if len(fpCols) >= 4 {
			break
		}
		if isNumericType(f.Type.ID()) && bmap[f.Name] == amap[f.Name] {
			if _, ok := bmap[f.Name]; ok {
				fpCols = append(fpCols, f.Name)
			}
		}
	}
	for _, c := range fpCols {
		q := quoteIdent(c)
		compare("Σ "+c, fmt.Sprintf("SELECT COUNT(%s), AVG(%s) FROM %s", q, q, texpr),
			func(r []string) string {
				if len(r) >= 2 {
					return "count " + groupDigits(r[0]) + " · avg " + r[1]
				}
				return "(empty)"
			}, true)
	}

	// ── verdict ─────────────────────────────────────────────────────────
	same, differ := 0, 0
	for _, c := range rep.Checks {
		if c.Same {
			same++
		} else {
			differ++
		}
	}
	rep.Same = differ == 0
	if jsonOut {
		b, _ := json.MarshalIndent(rep, "", "  ")
		fmt.Println(string(b))
	} else {
		fmt.Println()
		if rep.Same {
			fmt.Printf("%d checks — identical\n", same)
		} else {
			fmt.Printf("%d same · %d differ — drift detected\n", same, differ)
		}
	}
	if !rep.Same {
		return fmt.Errorf("diff: %d difference(s) between %s and %s", differ, aname, bname)
	}
	return nil
}

func diffSchema(ctx context.Context, cl *flightsql.Client, texpr string) (*arrow.Schema, error) {
	info, err := cl.Execute(ctx, "SELECT * FROM "+texpr+" LIMIT 0")
	if err != nil {
		return nil, err
	}
	if len(info.Schema) > 0 {
		if sc, err := flight.DeserializeSchema(info.Schema, memory.DefaultAllocator); err == nil {
			return sc, nil
		}
	}
	for _, ep := range info.Endpoint {
		rdr, err := cl.DoGet(ctx, ep.Ticket)
		if err != nil {
			return nil, err
		}
		sc := rdr.Schema()
		for rdr.Next() {
		}
		rdr.Release()
		if sc != nil {
			return sc, nil
		}
	}
	return nil, fmt.Errorf("no schema in FlightInfo or stream")
}

func fieldTypes(s *arrow.Schema) map[string]string {
	out := make(map[string]string, s.NumFields())
	for _, f := range s.Fields() {
		out[f.Name] = f.Type.String()
	}
	return out
}

func renderOr(row []string, render func([]string) string) string {
	if row == nil {
		return "(unavailable)"
	}
	return render(row)
}

// rowsEqual compares result rows; numeric cells get a relative tolerance so
// engine-specific float summation order doesn't read as drift.
func rowsEqual(a, b []string, numeric bool) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] == b[i] {
			continue
		}
		if numeric {
			fa, ea := strconv.ParseFloat(a[i], 64)
			fb, eb := strconv.ParseFloat(b[i], 64)
			if ea == nil && eb == nil &&
				math.Abs(fa-fb) <= 1e-9*math.Max(1, math.Max(math.Abs(fa), math.Abs(fb))) {
				continue
			}
		}
		return false
	}
	return true
}
