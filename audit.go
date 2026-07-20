// audit — a security-surface probe for a Flight SQL server you operate.
//
// A Flight SQL client sends arbitrary SQL, and a DuckDB-backed server runs it
// on a connection with DuckDB's full default powers: reading host files,
// listing directories, writing files, fetching URLs (SSRF), and changing
// server-wide configuration (raise memory_limit to OOM the node, or re-enable
// any of the above). This command probes each of those with a BENIGN version
// — read /etc/hostname, list /, write /dev/null, connect to a dead loopback
// port, flip an inert setting — and reports which the server permits.
//
// Run it against a server you operate or are explicitly authorized to test.
// It is a defender's tool: it verifies the hardening
// (enable_external_access=false · allowed_directories · lock_configuration),
// it does not exploit. Note: on an unhardened DuckDB server the net probe
// makes the server autoload the httpfs extension (a one-time download).
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"
)

type auditFinding struct {
	Probe   string `json:"probe"`
	Verdict string `json:"verdict"` // exposed | blocked | n/a | unknown
	Detail  string `json:"detail"`
	Hint    string `json:"hint,omitempty"`
}

type auditReport struct {
	Endpoint string         `json:"endpoint"`
	Profile  string         `json:"profile"`
	Findings []auditFinding `json:"findings"`
	Exposed  int            `json:"exposed"`
}

// each probe is the least-harmful SQL that still proves the capability
var auditProbes = []struct {
	name, sql, risk, hint, kind string
}{
	{"file-read", "SELECT length(content) FROM read_text('/etc/hostname')",
		"client SQL can read arbitrary files off the server host",
		"DuckDB: SET enable_external_access=false (+ allowed_directories, lock_configuration)", "read"},
	{"dir-list", "SELECT count(*) FROM glob('/*')",
		"client SQL can enumerate the server's filesystem",
		"same fix as file-read", "read"},
	{"file-write", "COPY (SELECT 1 AS x) TO '/dev/null' (FORMAT CSV)",
		"client SQL can write files on the server host",
		"same fix as file-read", "read"},
	{"net-fetch", "SELECT 1 FROM read_csv('http://127.0.0.1:1/probe.csv')",
		"client SQL can make the server open outbound connections (SSRF — a real attacker aims at cloud metadata or internal hosts)",
		"block httpfs autoload with SET enable_external_access=false", "net"},
	{"ext-load", "LOAD spatial",
		"client SQL can load DuckDB extensions — arbitrary native code, and the gateway to httpfs/SSRF",
		"DuckDB: SET allow_community_extensions=false + autoload_known_extensions=false", "read"},
	{"config-write", "SET enable_progress_bar=false",
		"client SQL can change server-wide DuckDB config (raise memory_limit to OOM the node, or undo hardening)",
		"DuckDB: SET lock_configuration=true after all startup config", "exec"},
}

func classifyAudit(err error, kind string) string {
	if err == nil {
		return "exposed" // the operation was permitted and completed
	}
	s := strings.ToLower(err.Error())
	for _, b := range []string{"disabled by configuration", "configuration has been locked",
		"permission denied", "permission error", "not allowed", "forbidden",
		"read-only", "read only", "not accepted"} {
		if strings.Contains(s, b) {
			return "blocked"
		}
	}
	for _, n := range []string{"does not exist", "not found", "unknown function",
		"syntax error", "unimplemented", "not implemented", "catalog error", "invalid function",
		"parsererror", "unexpected token", "unsupported configuration", "could not find config",
		"no function matches", "schemaerror"} {
		if strings.Contains(s, n) {
			return "n/a"
		}
	}
	// an error that proves the primitive ran (access wasn't denied by policy)
	if kind == "net" {
		for _, c := range []string{"connection", "could not", "io error", "http",
			"timeout", "refused", "no route", "failed to connect", "connect"} {
			if strings.Contains(s, c) {
				return "exposed"
			}
		}
	}
	if kind == "read" {
		for _, c := range []string{"no files found", "cannot open", "io error"} {
			if strings.Contains(s, c) {
				return "exposed"
			}
		}
	}
	return "unknown"
}

var auditMark = map[string]string{"exposed": "✗", "blocked": "✓", "n/a": "·", "unknown": "⚠"}

func cmdAudit(args []string) error {
	fs := newFlagSet("audit", `usage: sparrow audit [flags]
security-surface audit: what can a client with SQL access do BEYOND querying?
Probes file reads, directory listing, file writes, outbound fetch (SSRF),
server-config changes, and catalog writes (CREATE/DROP). Every probe is benign
— reads /etc/hostname, writes /dev/null, connects to a dead loopback port,
flips an inert setting, and round-trips a throwaway table (created then dropped).
Run ONLY against a server you operate or are authorized to test.
Exit 1 if any surface is exposed — so it gates a deploy.
examples: sparrow audit · sparrow audit -s prod -o json`)
	cf := addConnFlags(fs)
	output := fs.String("o", "", `output: "json" for a machine-readable report`)
	parseFlags(fs, args)
	jsonOut := false
	switch strings.ToLower(*output) {
	case "":
	case "json":
		jsonOut = true
	default:
		return usagef(`audit -o supports only "json"`)
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

	rep := auditReport{Endpoint: p.URI, Profile: pname}
	if !jsonOut {
		fmt.Printf("sparrow audit — %s (profile: %s)\n", p.URI, pname)
		fmt.Print("probing what client SQL can reach beyond the query interface\n\n")
	}
	naCount := 0
	emit := func(f auditFinding) {
		rep.Findings = append(rep.Findings, f)
		if !jsonOut {
			fmt.Printf(" %s %-13s %s\n", auditMark[f.Verdict], f.Probe, f.Detail)
			if f.Hint != "" {
				fmt.Println("               hint: " + f.Hint)
			}
		}
	}
	for _, pr := range auditProbes {
		pctx, pcancel := context.WithTimeout(ctx, 20*time.Second)
		_, e := queryRow(pctx, cl, pr.sql)
		pcancel()
		v := classifyAudit(e, pr.kind)
		f := auditFinding{Probe: pr.name, Verdict: v}
		switch v {
		case "exposed":
			f.Detail, f.Hint = pr.risk, pr.hint
			rep.Exposed++
		case "blocked":
			f.Detail = "blocked by the server"
		case "n/a":
			f.Detail = "not reachable (hardened, or a non-DuckDB engine)"
			naCount++
		default:
			f.Detail = "could not determine: " + firstLine(e)
		}
		emit(f)
	}

	// catalog-write probe (tester S1, 2026-07-18): can a client CREATE a
	// persistent object in the server's writable catalog? That's what let a
	// client DROP the FTS backing store out from under search_meta. Benign
	// round-trip: CREATE OR REPLACE a uniquely-named table, then drop it. A
	// read-only-enforcing server refuses the CREATE (→ blocked); a stock
	// DuckDB server permits it (→ exposed). Cleanup is best-effort but the
	// CREATE only runs when the server ALLOWS writes, so a leftover means the
	// server is exposed anyway (and IF NOT... the DROP mirrors the create).
	{
		const probe = "__sparrow_audit_catwrite"
		pctx, pcancel := context.WithTimeout(ctx, 20*time.Second)
		_, ce := queryRow(pctx, cl, "CREATE OR REPLACE TABLE "+probe+" (x INTEGER)")
		pcancel()
		v := classifyAudit(ce, "exec")
		f := auditFinding{Probe: "catalog-write", Verdict: v}
		switch v {
		case "exposed":
			f.Detail = "client SQL can CREATE/DROP objects in the server's catalog (can corrupt server-owned tables — e.g. an FTS index — or fill memory)"
			f.Hint = "open the client-facing DuckDB catalog read-only, or gate DDL (enforce single-SELECT); isolate any server-owned tables from the client-writable catalog"
			rep.Exposed++
			// clean up the object we just created
			dctx, dcancel := context.WithTimeout(ctx, 20*time.Second)
			queryRow(dctx, cl, "DROP TABLE IF EXISTS "+probe)
			dcancel()
		case "blocked":
			f.Detail = "DDL refused — catalog is not client-writable"
		case "n/a":
			f.Detail = "not reachable (non-DuckDB engine, or no writable catalog)"
			naCount++
		default:
			f.Detail = "could not determine: " + firstLine(ce)
		}
		emit(f)
	}

	if jsonOut {
		b, _ := json.MarshalIndent(rep, "", "  ")
		fmt.Println(string(b))
	} else {
		fmt.Println()
		switch {
		case rep.Exposed > 0:
			fmt.Printf("%d exposed — this server lets client SQL reach beyond querying\n", rep.Exposed)
		case naCount == len(auditProbes)+1: // +1 for the catalog-write probe
			fmt.Println("no DuckDB file/network primitives reachable — hardened, or not a DuckDB-backed server")
		default:
			fmt.Println("clean — no exposed surface found")
		}
	}
	if rep.Exposed > 0 {
		return fmt.Errorf("audit: %d exposed surface(s) — see the ✗ lines", rep.Exposed)
	}
	return nil
}
