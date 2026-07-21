// receipt — verifiable data provenance. `sparrow sql "..." --receipt r.json`
// writes a manifest: the query, the server's identity, a timestamp, and an
// order-independent content fingerprint of the result. `sparrow verify r.json`
// re-runs the query against the server and confirms the fingerprint still
// matches — proof that a number REALLY came from that query against that
// server, and wasn't invented.
//
// The fingerprint is computed server-side and is order-independent: count(*)
// plus sum(hash(all cols)) and bit_xor(hash(all cols)). Two independent digests
// over the row multiset — no download, no ORDER BY needed, and duplicate rows
// don't cancel (sum adds where xor would xor to zero). A query that isn't
// deterministic server-side (now(), random()) won't verify — which is correct:
// a receipt proves reproducibility.
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/apache/arrow-go/v18/arrow/flight/flightsql"
)

const receiptVersion = "1"
const receiptAlgo = "duckdb-hash-v1"

// isoNow is the receipt timestamp (informational — verification never depends
// on it, so clock skew is harmless).
func isoNow() string { return time.Now().UTC().Format(time.RFC3339) }

type receiptServer struct {
	Endpoint string `json:"endpoint"`
	Vendor   string `json:"vendor,omitempty"`
}

type receiptResult struct {
	Rows      int64    `json:"rows"`
	Columns   []string `json:"columns"`
	DigestSum string   `json:"digest_sum"`
	DigestXor string   `json:"digest_xor"`
}

type receipt struct {
	Kind    string        `json:"sparrow_receipt"` // version tag
	Algo    string        `json:"algo"`
	Query   string        `json:"query"`
	Server  receiptServer `json:"server"`
	Created string        `json:"created"` // RFC3339; informational
	Result  receiptResult `json:"result"`
}

// fingerprint computes the order-independent content digest of a query's result
// server-side. Returns rows, sum-digest, xor-digest, and the column names.
func fingerprint(ctx context.Context, cl *flightsql.Client, query string) (receiptResult, error) {
	var res receiptResult
	cols, err := querySchemaCols(ctx, cl, query)
	if err != nil {
		return res, err // raw — caller classifies auth vs query error
	}
	res.Columns = cols
	if len(cols) == 0 {
		return res, fmt.Errorf("query returns no columns")
	}
	quoted := make([]string, len(cols))
	for i, c := range cols {
		quoted[i] = quoteIdent(c)
	}
	hashExpr := "hash(" + strings.Join(quoted, ", ") + ")"
	inner := strings.TrimRight(query, "; \t\r\n")
	fpSQL := fmt.Sprintf(
		"SELECT count(*), coalesce(sum(%s)::VARCHAR, '0'), coalesce(bit_xor(%s)::VARCHAR, '0') FROM (%s) AS __receipt",
		hashExpr, hashExpr, inner)
	row, err := queryRow(ctx, cl, fpSQL)
	if err != nil {
		return res, err // raw — caller classifies
	}
	if len(row) < 3 {
		return res, fmt.Errorf("fingerprint: unexpected result shape")
	}
	res.Rows, _ = strconv.ParseInt(strings.TrimSpace(row[0]), 10, 64)
	res.DigestSum = strings.TrimSpace(row[1])
	res.DigestXor = strings.TrimSpace(row[2])
	return res, nil
}

// writeReceipt dials, fingerprints the query, and writes the manifest. Called
// by sql/query when --receipt is given (a short second connection after the
// stream — the receipt captures the result as computed at receipt time).
func writeReceipt(cf *connFlags, query, path, created string) error {
	p, _, err := cf.resolve()
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	cl, ctx, err := dial(ctx, p)
	if err != nil {
		return connError{err}
	}
	defer cl.Close()

	res, err := fingerprint(ctx, cl, query)
	if err != nil {
		return fmt.Errorf("--receipt: %v", err)
	}
	r := receipt{
		Kind:    receiptVersion,
		Algo:    receiptAlgo,
		Query:   strings.TrimSpace(query),
		Server:  receiptServer{Endpoint: p.URI, Vendor: probeVendor(ctx, cl)},
		Created: created,
		Result:  res,
	}
	b, err := json.MarshalIndent(r, "", "  ")
	if err != nil {
		return err
	}
	if err := os.WriteFile(path, append(b, '\n'), 0o644); err != nil {
		return err
	}
	fmt.Fprintf(os.Stderr, "receipt: %s rows, digest written to %s\n",
		groupDigits(strconv.FormatInt(res.Rows, 10)), path)
	return nil
}

func cmdVerify(args []string) error {
	fs := newFlagSet("verify", `usage: sparrow verify <receipt.json> [-s profile]
Re-run a receipt's query against the server and confirm the result fingerprint
still matches — proof a number came from that query against that server. Exit 0
if it verifies, 1 if the data changed (or the query is non-deterministic), 2 on
a connection failure. By default it targets the receipt's own endpoint; -s
overrides (verify the same query against a different server).
examples: sparrow verify wti.receipt.json
          sparrow verify wti.receipt.json -s gizmo   # does another server agree?`)
	cf := addConnFlags(fs)
	output := fs.String("o", "", `output: "json" for a machine-readable verdict`)
	pos := parseFlags(fs, args)
	if len(pos) != 1 {
		return usagef("usage: sparrow verify <receipt.json>")
	}
	jsonOut := false
	switch strings.ToLower(*output) {
	case "":
	case "json":
		jsonOut = true
	default:
		return usagef(`verify -o supports only "json"`)
	}

	raw, err := os.ReadFile(pos[0])
	if err != nil {
		return usagef("verify: cannot read %s: %s", pos[0], firstLine(err))
	}
	var r receipt
	if err := json.Unmarshal(raw, &r); err != nil {
		return usagef("verify: %s is not a sparrow receipt (bad JSON): %s", pos[0], firstLine(err))
	}
	if r.Kind == "" || r.Query == "" {
		return usagef("verify: %s is not a sparrow receipt (missing sparrow_receipt/query)", pos[0])
	}
	// R4: a structurally-incomplete receipt is a BAD RECEIPT (exit 3), not a
	// "data changed" (exit 1) — an agent branching on the code must tell them
	// apart. R5: an unknown fingerprint algorithm can't be verified by this
	// binary; refuse rather than silently verify with the wrong algo.
	if r.Result.DigestSum == "" || r.Result.DigestXor == "" {
		return usagef("verify: %s is an incomplete receipt (missing result fingerprint)", pos[0])
	}
	if r.Algo != "" && r.Algo != receiptAlgo {
		return usagef("verify: receipt algo %q is not supported by this binary (%s) — upgrade sparrow", r.Algo, receiptAlgo)
	}

	// Endpoint + auth resolution (R3):
	//  -s given            → use that profile/URI as-is (a deliberate other server).
	//  no -s               → aim at the receipt's endpoint, and find creds by
	//                        (1) a saved profile whose URI matches it, then
	//                        (2) standalone --basic/--bearer/TLS flags on top.
	usedOverride := *cf.server != ""
	var p Profile
	if usedOverride {
		var perr error
		if p, _, perr = cf.resolve(); perr != nil {
			return perr
		}
	} else {
		if m, ok := profileByURI(r.Server.Endpoint); ok {
			p = m
		} else {
			p = Profile{Auth: "none"}
		}
		p.URI = r.Server.Endpoint
		if *cf.basic != "" {
			u, pw, _ := strings.Cut(*cf.basic, ":")
			p.Auth, p.User, p.Pass = "basic", u, pw
		}
		if *cf.bearer != "" {
			p.Auth, p.Token = "bearer", *cf.bearer
		}
		if *cf.tlsSkip {
			p.TLSSkipVerify = true
		}
		if *cf.cert != "" {
			p.TLSCert = *cf.cert
		}
		if *cf.key != "" {
			p.TLSKey = *cf.key
		}
		if *cf.ca != "" {
			p.TLSCA = *cf.ca
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	cl, ctx, err := dial(ctx, p)
	if err != nil {
		return connError{err}
	}
	defer cl.Close()

	nowVendor := probeVendor(ctx, cl)
	res, err := fingerprint(ctx, cl, r.Query)
	if err != nil {
		return classifyConnErr(fmt.Errorf("verify: %s", firstLine(err)), err) // R3c: auth/unreachable → exit 2
	}

	rowsMatch := res.Rows == r.Result.Rows
	sumMatch := res.DigestSum == r.Result.DigestSum
	xorMatch := res.DigestXor == r.Result.DigestXor
	digestMatch := sumMatch && xorMatch
	// R2: bind result shape too (skip only when the receipt predates columns).
	colsMatch := len(r.Result.Columns) == 0 || equalStrings(r.Result.Columns, res.Columns)
	// R1: a GENUINE vendor comparison, always. Server identity gates the verdict
	// only when verifying against the receipt's OWN endpoint (no -s); an -s
	// override deliberately targets a different server, so we report the vendor
	// but don't fail on it (the point there is cross-server DATA agreement).
	vendorMatch := nowVendor == r.Server.Vendor
	dataOK := rowsMatch && digestMatch && colsMatch
	ok := dataOK && (usedOverride || vendorMatch)

	if jsonOut {
		rep := map[string]any{
			"ok":            ok,
			"endpoint":      p.URI,
			"override":      usedOverride,
			"vendor_now":    nowVendor,
			"vendor_recept": r.Server.Vendor,
			"vendor_match":  vendorMatch,
			"rows_match":    rowsMatch,
			"digest_match":  digestMatch,
			"columns_match": colsMatch,
			"receipt_rows":  r.Result.Rows,
			"now_rows":      res.Rows,
		}
		b, _ := json.MarshalIndent(rep, "", "  ")
		fmt.Println(string(b))
	} else {
		mark := func(b bool) string {
			if b {
				return "✓"
			}
			return "✗"
		}
		fmt.Printf("verify %s\n", pos[0])
		fmt.Printf(" endpoint   %s%s\n", p.URI, map[bool]string{true: " (-s override)", false: " (from receipt)"}[usedOverride])
		if usedOverride {
			// vendor reported, not gated — you chose this server
			fmt.Printf(" · server    %s%s\n", nowVendor, mismatchNote(vendorMatch, r.Server.Vendor))
		} else {
			fmt.Printf(" %s server    %s%s\n", mark(vendorMatch), nowVendor, mismatchNote(vendorMatch, r.Server.Vendor))
		}
		fmt.Printf(" %s rows       %s%s\n", mark(rowsMatch), groupDigits(strconv.FormatInt(res.Rows, 10)),
			mismatchNote(rowsMatch, groupDigits(strconv.FormatInt(r.Result.Rows, 10))))
		fmt.Printf(" %s columns    %s\n", mark(colsMatch),
			map[bool]string{true: strings.Join(res.Columns, ", "), false: "DIFFER from the receipt"}[colsMatch])
		fmt.Printf(" %s digest     %s\n", mark(digestMatch),
			map[bool]string{true: "matches", false: "DIFFERS — the result changed since the receipt"}[digestMatch])
	}
	if !ok {
		return fmt.Errorf("verify: FAILED — %s", failReason(dataOK, vendorMatch, usedOverride))
	}
	return nil
}

func failReason(dataOK, vendorMatch, override bool) string {
	if !dataOK {
		return "the result does not match the receipt"
	}
	if !override && !vendorMatch {
		return "the endpoint no longer identifies as the receipt's server"
	}
	return "verification failed"
}

// profileByURI finds a saved profile whose endpoint URI matches — so a bare
// `verify r.json` picks up the creds for the receipt's server automatically.
func profileByURI(uri string) (Profile, bool) {
	if uri == "" {
		return Profile{}, false
	}
	for _, p := range loadConfig().Profiles {
		if p.URI == uri {
			return p, true
		}
	}
	return Profile{}, false
}

// classifyConnErr returns a connError (exit 2) when the underlying error is an
// auth/connection failure, else the display error as-is (exit 1). R3c: verify's
// auth failures must be exit 2 whether or not -s was used.
func classifyConnErr(display, raw error) error {
	s := strings.ToLower(raw.Error())
	for _, m := range []string{"unauthenticated", "permissiondenied", "permission denied",
		"unavailable", "connection refused", "no route", "deadline exceeded", "invalid_argument: auth"} {
		if strings.Contains(s, m) {
			return connError{display}
		}
	}
	return display
}

func mismatchNote(match bool, want string) string {
	if match {
		return ""
	}
	return "  (receipt: " + want + ")"
}
