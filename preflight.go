// preflight — result-size awareness and enforcement, so an agent can size a
// pull BEFORE it floods its own context or hammers a server.
//
//	--cost   (sql/query): estimate rows + decoded bytes WITHOUT streaming the
//	         result — count(*) for rows, a first-batch bytes/row extrapolation
//	         for size. The "how much" sibling of --schema's "what shape".
//	--budget (sql/query/pull/head): a hard ceiling — "10MB" | "5000rows" |
//	         "30s". The stream is aborted the instant it crosses the ceiling,
//	         with a clean error (exit 1). estimate -> decide -> enforce.
package main

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/flight/flightsql"
)

// recordBytes sums the decoded (in-memory) size of every column in a record.
func recordBytes(rec arrow.Record) int64 {
	var n int64
	for _, col := range rec.Columns() {
		n += arrayDataSize(col.Data())
	}
	return n
}

// budget is a hard streaming ceiling. Zero fields are unset; set is true once
// any field is armed. A stream that crosses an armed ceiling is aborted.
type budget struct {
	rows  int64
	bytes int64
	dur   time.Duration
	set   bool
	raw   string
}

// parseBudget reads "10MB" | "512KB" | "1GB" | "5000rows" | "5000" | "30s" |
// "500ms". A bare number means rows. Multiple comma-separated ceilings AND
// together (any one trips the abort): --budget 10MB,30s.
func parseBudget(spec string) (budget, error) {
	b := budget{raw: spec}
	for _, part := range strings.Split(spec, ",") {
		s := strings.TrimSpace(strings.ToLower(part))
		if s == "" {
			continue
		}
		switch {
		case hasAnySuffix(s, "rows", "row"): // before "ms"/"s" — "rows" ends in "s"
			n, err := strconv.ParseInt(trimAnySuffix(s, "rows", "row"), 10, 64)
			if err != nil {
				return b, fmt.Errorf("--budget: bad row count %q", part)
			}
			b.rows = n
		case strings.HasSuffix(s, "ms"):
			ms, err := strconv.Atoi(strings.TrimSuffix(s, "ms"))
			if err != nil {
				return b, fmt.Errorf("--budget: bad duration %q", part)
			}
			b.dur = time.Duration(ms) * time.Millisecond
		case hasAnySuffix(s, "seconds", "second", "secs", "sec", "s"):
			sec, err := strconv.ParseFloat(trimAnySuffix(s, "seconds", "second", "secs", "sec", "s"), 64)
			if err != nil {
				return b, fmt.Errorf("--budget: bad duration %q", part)
			}
			b.dur = time.Duration(sec * float64(time.Second))
		case strings.HasSuffix(s, "gb"):
			b.bytes = parseUnit(s, "gb", 1e9)
		case strings.HasSuffix(s, "mb"):
			b.bytes = parseUnit(s, "mb", 1e6)
		case strings.HasSuffix(s, "kb"):
			b.bytes = parseUnit(s, "kb", 1e3)
		case strings.HasSuffix(s, "b"):
			b.bytes = parseUnit(s, "b", 1)
		default:
			n, err := strconv.ParseInt(s, 10, 64)
			if err != nil {
				return b, fmt.Errorf("--budget: %q is not a size (10MB), row count (5000rows), or duration (30s)", part)
			}
			b.rows = n // bare number = rows
		}
		if b.bytes < 0 {
			return b, fmt.Errorf("--budget: bad size %q", part)
		}
	}
	b.set = b.rows > 0 || b.bytes > 0 || b.dur > 0
	if !b.set {
		return b, fmt.Errorf("--budget: empty or zero ceiling %q", spec)
	}
	return b, nil
}

// hasAnySuffix / trimAnySuffix let one unit accept singular + long spellings
// (5rows/5row, 30s/30sec/30seconds) — tester B1 (2026-07-21). Longest first so
// "second" wins over "s".
func hasAnySuffix(s string, suffixes ...string) bool {
	for _, suf := range suffixes {
		if strings.HasSuffix(s, suf) {
			return true
		}
	}
	return false
}

func trimAnySuffix(s string, suffixes ...string) string {
	for _, suf := range suffixes {
		if strings.HasSuffix(s, suf) {
			return strings.TrimSuffix(s, suf)
		}
	}
	return s
}

func parseUnit(s, suffix string, mult float64) int64 {
	f, err := strconv.ParseFloat(strings.TrimSuffix(s, suffix), 64)
	if err != nil {
		return -1
	}
	return int64(f * mult)
}

// budgetError signals a --budget ceiling was hit; main() maps it to exit 1
// (a gate hit, like a check finding), distinct from a connection failure.
type budgetError struct{ msg string }

func (e budgetError) Error() string { return e.msg }

// preflightCost estimates a SQL query's result size without streaming it:
// count(*) gives the exact row total, a first-batch sample gives bytes/row.
// Reports to stderr; nothing goes to stdout. Runs against whatever profile is
// resolved — remember heavy count(*)s belong on a fixture, not a prod node.
func preflightCost(ctx context.Context, cl *flightsql.Client, query, format string) error {
	inner := strings.TrimRight(query, "; \t\r\n")

	// exact rows
	row, err := queryRow(ctx, cl, "SELECT count(*) FROM ("+inner+") AS __cost")
	if err != nil {
		return fmt.Errorf("cost: %s", firstLine(err))
	}
	var totalRows int64
	if len(row) > 0 {
		totalRows, _ = strconv.ParseInt(strings.TrimSpace(row[0]), 10, 64)
	}

	// bytes/row from the first batch of the real query
	info, err := cl.Execute(ctx, inner)
	if err != nil {
		return fmt.Errorf("cost: %s", firstLine(err))
	}
	var batchRows, batchBytes int64
	if len(info.Endpoint) > 0 {
		rdr, err := cl.DoGet(ctx, info.Endpoint[0].Ticket)
		if err != nil {
			return fmt.Errorf("cost: %s", firstLine(err))
		}
		if rdr.Next() {
			rec := rdr.Record()
			batchRows = rec.NumRows()
			batchBytes = recordBytes(rec)
		}
		rdr.Release()
	}

	var estBytes int64
	perRow := "?"
	if batchRows > 0 {
		bpr := float64(batchBytes) / float64(batchRows)
		estBytes = int64(bpr * float64(totalRows))
		perRow = fmt.Sprintf("%.0f B/row", bpr)
	}

	fmt.Fprintf(os.Stderr, "── cost estimate ───────────────────────\n")
	fmt.Fprintf(os.Stderr, "rows       %s (exact, via count(*))\n", groupDigits(strconv.FormatInt(totalRows, 10)))
	if estBytes > 0 {
		fmt.Fprintf(os.Stderr, "decoded    ~%s (%s × %s rows, from a first-batch sample)\n",
			fmtBytes(estBytes), perRow, groupDigits(strconv.FormatInt(totalRows, 10)))
		fmt.Fprintf(os.Stderr, "wire       ~%s with lz4 (Sparrow pull compresses by default; typical 2–3.5×)\n", fmtBytes(estBytes/3))
	} else {
		fmt.Fprintln(os.Stderr, "decoded    unknown (empty result — no batch to sample)")
	}
	// the md-cap advisory: the one that actually protects an agent's context
	cap1000 := format == "md" || format == ""
	if cap1000 && totalRows > 1000 {
		fmt.Fprintf(os.Stderr, "advisory   exceeds the 1000-row -o md cap — add a LIMIT, pull -o a file, or raise --max-rows\n")
	}
	fmt.Fprintf(os.Stderr, "note       estimate only — nothing streamed; run the query to fetch\n")
	return nil
}
