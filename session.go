// session — reproducible investigations. Set SPARROW_SESSION=path and every
// read (sql/query/pull/head) appends a JSONL record: the command, the endpoint,
// the query/ticket, the row count, and — for SQL — the same order-independent
// content fingerprint receipts use. `sparrow replay <session.jsonl>` re-runs the
// recorded reads and diffs each against its fingerprint, so "here's how I got
// this number" becomes "…and it still reproduces." Investigation-as-regression-
// test — the reusable-ticket idea lifted from one query to a whole exploration.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/apache/arrow-go/v18/arrow/flight/flightsql"
)

type sessionStep struct {
	TS       string         `json:"ts"`
	Argv     []string       `json:"argv"`
	Endpoint string         `json:"endpoint"`
	Kind     string         `json:"kind"`             // "sql" | "pull"
	Query    string         `json:"query,omitempty"`  // SQL text (sql/query)
	Ticket   string         `json:"ticket,omitempty"` // raw ticket JSON (pull)
	Rows     int64          `json:"rows"`
	Ms       int64          `json:"ms"`
	FP       *receiptResult `json:"fingerprint,omitempty"` // present when a SQL fingerprint was computed
}

func sessionPath() string { return strings.TrimSpace(os.Getenv("SPARROW_SESSION")) }

// redactArgv masks the values of credential-bearing flags (--basic user:pass,
// --bearer token, --encrypt-key, --header k=v) in the recorded argv. A session
// file is meant to be handed around and replayed by someone else — secrets must
// not ride along. Argv is informational only; replay never re-parses it.
func redactArgv(args []string) []string {
	sensitive := func(name string) bool {
		switch strings.TrimLeft(name, "-") {
		case "basic", "bearer", "encrypt-key", "header":
			return true
		}
		return false
	}
	out := make([]string, len(args))
	for i := 0; i < len(args); i++ {
		a := args[i]
		if k, _, found := strings.Cut(a, "="); found && strings.HasPrefix(a, "-") && sensitive(k) {
			out[i] = k + "=***"
			continue
		}
		out[i] = a
		if strings.HasPrefix(a, "-") && sensitive(a) && i+1 < len(args) {
			i++
			out[i] = "***"
		}
	}
	return out
}

// recordSession appends one step to the SPARROW_SESSION file (no-op if unset).
// For SQL it also computes the content fingerprint (one extra aggregate on the
// already-open connection) so replay can verify, not just re-count. Best-effort:
// a recording failure never fails the command — it warns to stderr.
func recordSession(ctx context.Context, cl *flightsql.Client, query string, ticket []byte, endpoint string, rows, ms int64) {
	path := sessionPath()
	if path == "" {
		return
	}
	step := sessionStep{
		TS: isoNow(), Argv: redactArgv(os.Args[1:]), Endpoint: endpoint, Rows: rows, Ms: ms,
	}
	if query != "" {
		step.Kind, step.Query = "sql", strings.TrimSpace(query)
		if fp, err := fingerprint(ctx, cl, query); err == nil {
			step.FP = &fp
			step.Rows = fp.Rows // the fingerprint's count is what replay compares
		}
	} else if ticket != nil {
		step.Kind, step.Ticket = "pull", string(ticket)
	} else {
		return // nothing reproducible to record
	}
	line, err := json.Marshal(step)
	if err != nil {
		return
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		fmt.Fprintf(os.Stderr, "note: SPARROW_SESSION record failed (%s)\n", firstLine(err))
		return
	}
	defer f.Close()
	f.Write(append(line, '\n'))
}

func cmdReplay(args []string) error {
	fs := newFlagSet("replay", `usage: sparrow replay <session.jsonl> [-s profile]
Re-run a recorded session and confirm each step still reproduces. A session is
written by any command run with SPARROW_SESSION=<file> set — every read appends
a step (query + endpoint + row count + a content fingerprint for SQL). replay
re-runs each SQL step and diffs its fingerprint: exit 0 if the whole
investigation reproduces, 1 if any step drifted, 2 if a server couldn't be
reached or authenticated. -s overrides every step's endpoint (replay the
investigation against a different server).
examples: SPARROW_SESSION=probe.jsonl sparrow sql "SELECT count(*) FROM series_data"
          sparrow replay probe.jsonl                 # …does it still hold?
          sparrow replay probe.jsonl -s gizmo        # …on another server?`)
	cf := addConnFlags(fs)
	output := fs.String("o", "", `output: "json" for a machine-readable report`)
	pos := parseFlags(fs, args)
	if len(pos) != 1 {
		return usagef("usage: sparrow replay <session.jsonl>")
	}
	jsonOut := false
	switch strings.ToLower(*output) {
	case "":
	case "json":
		jsonOut = true
	default:
		return usagef(`replay -o supports only "json"`)
	}

	raw, err := os.ReadFile(pos[0])
	if err != nil {
		return usagef("replay: cannot read %s: %s", pos[0], firstLine(err))
	}
	var steps []sessionStep
	for i, ln := range strings.Split(string(raw), "\n") {
		ln = strings.TrimSpace(ln)
		if ln == "" {
			continue
		}
		var s sessionStep
		if err := json.Unmarshal([]byte(ln), &s); err != nil {
			return usagef("replay: %s line %d is not a session step: %s", pos[0], i+1, firstLine(err))
		}
		steps = append(steps, s)
	}
	if len(steps) == 0 {
		return usagef("replay: %s has no steps", pos[0])
	}

	usedOverride := *cf.server != ""
	type stepResult struct {
		Step       int    `json:"step"`
		Kind       string `json:"kind"`
		Verifiable bool   `json:"verifiable"`
		Reproduces bool   `json:"reproduces"`
		Detail     string `json:"detail"`
		RowsRecord int64  `json:"rows_recorded"`
		RowsNow    int64  `json:"rows_now"`
	}
	var results []stepResult
	failed, checked := 0, 0

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	// A fresh connection per step — replay is not a hot path, and reusing one
	// client across many fingerprint calls proved fragile with the demo's
	// Basic→Bearer auth adoption. Resolve creds the same way verify does:
	// -s overrides every step; otherwise a saved profile matching the step's
	// endpoint, else no auth.
	fpAt := func(endpoint, query string) (receiptResult, error) {
		var p Profile
		if usedOverride {
			var perr error
			if p, _, perr = cf.resolve(); perr != nil {
				return receiptResult{}, perr
			}
		} else if m, ok := profileByURI(endpoint); ok {
			p = m
		} else {
			p = Profile{Auth: "none", URI: endpoint}
		}
		if !usedOverride {
			p.URI = endpoint
		}
		cl, cctx, err := dial(ctx, p)
		if err != nil {
			return receiptResult{}, connError{err}
		}
		defer cl.Close()
		return fingerprint(cctx, cl, query)
	}

	for i, s := range steps {
		r := stepResult{Step: i + 1, Kind: s.Kind, RowsRecord: s.Rows}
		if s.FP == nil || s.Query == "" {
			r.Verifiable = false
			r.Detail = "not verifiable (no SQL fingerprint recorded)"
			results = append(results, r)
			continue
		}
		r.Verifiable = true
		checked++
		now, err := fpAt(s.Endpoint, s.Query)
		if err != nil {
			// Dial errors arrive wrapped in connError; bearer/no-auth failures
			// surface at fingerprint time instead (dial sends no RPC), so
			// classify those too (verify's R3c, applied here): couldn't check
			// is exit 2, NOT "didn't reproduce".
			var ce connError
			if errors.As(err, &ce) || errors.As(classifyConnErr(err, err), &ce) {
				return connError{fmt.Errorf("replay step %d: %s", i+1, firstLine(err))}
			}
			r.Reproduces, r.Detail = false, "re-run failed: "+firstLine(err)
			failed++
			results = append(results, r)
			continue
		}
		r.RowsNow = now.Rows
		reproduces := now.Rows == s.FP.Rows && now.DigestSum == s.FP.DigestSum &&
			now.DigestXor == s.FP.DigestXor && equalStrings(now.Columns, s.FP.Columns)
		r.Reproduces = reproduces
		if reproduces {
			r.Detail = "reproduces"
		} else if now.Rows != s.FP.Rows {
			r.Detail = fmt.Sprintf("row count changed (%s → %s)",
				groupDigits(strconv.FormatInt(s.FP.Rows, 10)), groupDigits(strconv.FormatInt(now.Rows, 10)))
			failed++
		} else {
			r.Detail = "content changed (same row count, different digest)"
			failed++
		}
		results = append(results, r)
	}

	if jsonOut {
		rep := map[string]any{
			"ok": failed == 0, "file": pos[0], "steps": len(steps),
			"checked": checked, "failed": failed, "results": results,
		}
		b, _ := json.MarshalIndent(rep, "", "  ")
		fmt.Println(string(b))
	} else {
		fmt.Printf("replay %s — %d steps\n", pos[0], len(steps))
		for _, r := range results {
			mark := "·"
			if r.Verifiable {
				mark = map[bool]string{true: "✓", false: "✗"}[r.Reproduces]
			}
			label := r.Kind
			if !r.Verifiable {
				label = r.Kind + " (not checked)"
			}
			fmt.Printf(" %s step %d  %-16s %s\n", mark, r.Step, label, r.Detail)
		}
		fmt.Printf("\n%d/%d verifiable steps reproduce", checked-failed, checked)
		if len(steps) > checked {
			fmt.Printf(" (%d step(s) not fingerprinted)", len(steps)-checked)
		}
		fmt.Println()
	}
	if failed > 0 {
		return fmt.Errorf("replay: %d of %d step(s) did not reproduce", failed, checked)
	}
	return nil
}
