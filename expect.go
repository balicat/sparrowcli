// expect — `sparrow expect "<sql>" --eq N | --rows 0 | --cols a,b …` turns any
// query into a self-verifying assertion: exit 0 if it holds, 1 if it doesn't.
//
// Where check/diff gate over FIXED dimensions (nulls, dup keys, drift), expect
// gates over an ARBITRARY query — the primitive an agent uses to pin a finding
// as a durable, replayable data contract:
//
//	sparrow expect "SELECT count(*) FROM series_data" --eq 136052269
//	sparrow expect "SELECT * FROM series_data WHERE value IS NULL" --empty
//	sparrow expect "SELECT * FROM search_meta('brent', lim:=5)" --cols series_id,name,description,score,total_matches
//
// Any combination of assertions may be given; ALL must hold. Row-count checks
// wrap the query in COUNT(*) (never materialized); scalar checks read the first
// cell of the first row; --cols reads the schema via a LIMIT 0 probe.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/apache/arrow-go/v18/arrow/flight/flightsql"
)

type expectResult struct {
	Assertion string `json:"assertion"`
	Expected  string `json:"expected"`
	Actual    string `json:"actual"`
	Pass      bool   `json:"pass"`
}

func cmdExpect(args []string) error {
	fs := newFlagSet("expect", `usage: sparrow expect "<sql>" <assertion...> [flags]
Assert something about a query's result; exit 0 if it holds, 1 if it doesn't —
the primitive for an agent to pin a finding as a replayable data contract.
SQL comes from the argument, -f file, or - (stdin). At least one assertion:
  scalar (first cell of the first row): --eq --ne --gt --lt --ge --le <v>  (numeric when both parse, else string)
  row count (wrapped in COUNT(*), not materialized): --rows N --rows-min N --rows-max N --empty --nonempty
  shape: --cols a,b,c  (result columns must be exactly these names, in order)
examples: sparrow expect "SELECT count(*) FROM series_data" --eq 136052269
          sparrow expect "SELECT * FROM t WHERE value IS NULL" --empty
          sparrow expect "SELECT avg(value) FROM series_data WHERE series_id='PET.RWTC.D'" --gt 0
          sparrow expect "SELECT * FROM search_meta('brent', lim:=3)" --cols series_id,name,description,score,total_matches`)
	cf := addConnFlags(fs)
	sqlFile := fs.String("f", "", "read the SQL from a file")
	eq := fs.String("eq", "", "assert the scalar equals this value")
	ne := fs.String("ne", "", "assert the scalar does NOT equal this value")
	gt := fs.String("gt", "", "assert the scalar is greater than this value")
	lt := fs.String("lt", "", "assert the scalar is less than this value")
	ge := fs.String("ge", "", "assert the scalar is >= this value")
	le := fs.String("le", "", "assert the scalar is <= this value")
	rows := fs.Int("rows", -1, "assert the result has exactly this many rows")
	rowsMin := fs.Int("rows-min", -1, "assert the result has at least this many rows")
	rowsMax := fs.Int("rows-max", -1, "assert the result has at most this many rows")
	empty := fs.Bool("empty", false, "assert the result has zero rows")
	nonempty := fs.Bool("nonempty", false, "assert the result has at least one row")
	cols := fs.String("cols", "", "assert the result columns are exactly these names, comma-separated, in order")
	output := fs.String("o", "", `output: "json" for a machine-readable report`)
	pos := parseFlags(fs, args)

	set := map[string]bool{}
	fs.Visit(func(f *flag.Flag) { set[f.Name] = true })

	// SQL source
	var query string
	switch {
	case *sqlFile != "":
		b, err := os.ReadFile(*sqlFile)
		if err != nil {
			return err
		}
		query = strings.TrimSpace(string(b))
	case len(pos) == 1 && pos[0] == "-":
		b, err := io.ReadAll(os.Stdin)
		if err != nil {
			return err
		}
		query = strings.TrimSpace(string(b))
	case len(pos) == 1:
		query = strings.TrimSpace(pos[0])
	default:
		return usagef(`usage: sparrow expect "<sql>" <assertion...>  (or -f file, or -)`)
	}
	if query == "" {
		return usagef("expect: empty SQL")
	}
	inner := strings.TrimRight(query, "; \t\r\n")

	jsonOut := false
	switch strings.ToLower(*output) {
	case "":
	case "json":
		jsonOut = true
	default:
		return usagef(`expect -o supports only "json"`)
	}

	needScalar := set["eq"] || set["ne"] || set["gt"] || set["lt"] || set["ge"] || set["le"]
	needCount := set["rows"] || set["rows-min"] || set["rows-max"] || *empty || *nonempty
	needCols := set["cols"]
	if !needScalar && !needCount && !needCols {
		return usagef("expect: give at least one assertion (--eq/--rows/--empty/--cols/…)")
	}

	p, pname, err := cf.resolve()
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	cl, ctx, err := dial(ctx, p)
	if err != nil {
		return connError{err}
	}
	defer cl.Close()

	var results []expectResult
	add := func(assertion, expected, actual string, pass bool) {
		results = append(results, expectResult{assertion, expected, actual, pass})
	}

	if needScalar {
		row, err := queryRow(ctx, cl, inner)
		if err != nil {
			return fmt.Errorf("expect: %s", firstLine(err))
		}
		if len(row) == 0 {
			// no rows → no scalar to compare; every scalar assertion fails
			for _, a := range []struct{ k, v string }{{"eq", *eq}, {"ne", *ne}, {"gt", *gt}, {"lt", *lt}, {"ge", *ge}, {"le", *le}} {
				if set[a.k] {
					add(a.k+" "+a.v, a.v, "<no rows>", false)
				}
			}
		} else {
			got := row[0]
			for _, a := range []struct{ k, v, op string }{
				{"eq", *eq, "=="}, {"ne", *ne, "!="}, {"gt", *gt, ">"},
				{"lt", *lt, "<"}, {"ge", *ge, ">="}, {"le", *le, "<="}} {
				if set[a.k] {
					add(fmt.Sprintf("scalar %s %s", a.op, a.v), a.v, got, compareScalar(got, a.v, a.k))
				}
			}
		}
	}

	if needCount {
		row, err := queryRow(ctx, cl, "SELECT count(*) FROM ("+inner+") AS __expect")
		if err != nil {
			return fmt.Errorf("expect: %s", firstLine(err))
		}
		n := 0
		if len(row) > 0 {
			n, _ = strconv.Atoi(strings.TrimSpace(row[0]))
		}
		got := strconv.Itoa(n)
		if set["rows"] {
			add(fmt.Sprintf("rows == %d", *rows), strconv.Itoa(*rows), got, n == *rows)
		}
		if set["rows-min"] {
			add(fmt.Sprintf("rows >= %d", *rowsMin), strconv.Itoa(*rowsMin), got, n >= *rowsMin)
		}
		if set["rows-max"] {
			add(fmt.Sprintf("rows <= %d", *rowsMax), strconv.Itoa(*rowsMax), got, n <= *rowsMax)
		}
		if *empty {
			add("rows == 0 (empty)", "0", got, n == 0)
		}
		if *nonempty {
			add("rows >= 1 (nonempty)", "1", got, n >= 1)
		}
	}

	if needCols {
		want := splitCols(*cols)
		names, err := querySchemaCols(ctx, cl, inner)
		if err != nil {
			return fmt.Errorf("expect: %s", firstLine(err))
		}
		add("cols == "+strings.Join(want, ","), strings.Join(want, ","),
			strings.Join(names, ","), equalStrings(want, names))
	}

	fails := 0
	for _, r := range results {
		if !r.Pass {
			fails++
		}
	}
	if jsonOut {
		rep := map[string]any{"ok": fails == 0, "endpoint": p.URI, "profile": pname, "assertions": results}
		b, _ := json.MarshalIndent(rep, "", "  ")
		fmt.Println(string(b))
	} else {
		for _, r := range results {
			mark := "✓"
			if !r.Pass {
				mark = "✗"
			}
			line := fmt.Sprintf(" %s %s", mark, r.Assertion)
			if !r.Pass {
				line += fmt.Sprintf("  (got %s)", r.Actual)
			}
			fmt.Println(line)
		}
	}
	if fails > 0 {
		return fmt.Errorf("expect: %d assertion(s) failed", fails)
	}
	return nil
}

// compareScalar evaluates one scalar assertion; numeric when both sides parse
// as float, otherwise a string comparison (eq/ne always meaningful).
func compareScalar(got, want, op string) bool {
	gf, gErr := strconv.ParseFloat(strings.TrimSpace(got), 64)
	wf, wErr := strconv.ParseFloat(strings.TrimSpace(want), 64)
	numeric := gErr == nil && wErr == nil
	switch op {
	case "eq":
		if numeric {
			return gf == wf
		}
		return got == want
	case "ne":
		if numeric {
			return gf != wf
		}
		return got != want
	case "gt":
		if numeric {
			return gf > wf
		}
		return got > want
	case "lt":
		if numeric {
			return gf < wf
		}
		return got < want
	case "ge":
		if numeric {
			return gf >= wf
		}
		return got >= want
	case "le":
		if numeric {
			return gf <= wf
		}
		return got <= want
	}
	return false
}

// querySchemaCols returns the result's column names via a LIMIT 0 probe — no
// rows fetched.
func querySchemaCols(ctx context.Context, cl *flightsql.Client, sql string) ([]string, error) {
	info, err := cl.Execute(ctx, "SELECT * FROM ("+sql+") AS __expect LIMIT 0")
	if err != nil {
		return nil, err
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
			names := make([]string, sc.NumFields())
			for i, f := range sc.Fields() {
				names[i] = f.Name
			}
			return names, nil
		}
	}
	return nil, fmt.Errorf("no schema returned")
}

func equalStrings(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
