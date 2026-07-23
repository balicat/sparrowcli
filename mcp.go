// mcp — `sparrow mcp` serves the CLI's core verbs over the Model Context
// Protocol (JSON-RPC 2.0 on stdio), so chat agents WITHOUT a shell — Claude
// Desktop, claude.ai integrations, Slack — can drive any Flight SQL server.
// The generic play, one hop further: point sparrow at a server and that
// server now speaks MCP.
//
// What the shape buys over shelling out to the CLI:
//   - a WARM connection: one dial + auth held across calls (a CLI invocation
//     re-dials every time — ~150 ms that swamps an 8 ms pull)
//   - schema-validated calls: SQL rides as a JSON string field — the entire
//     shell-quoting failure class is gone
//   - reach: hosts with no terminal at all
//
// Five tools in this first slice — orient, sql, pull, expect, verify — each a
// thin wrapper over the SAME internals the CLI commands use (orientMarkdown,
// queryRows, withAcceptCompression, compareScalar, fingerprint). Tool
// descriptions embed cmdDesc from completion.go so the catalog can't drift
// from the CLI's own vocabulary (mcp_test.go pins the correspondence).
//
// Protocol notes: newline-delimited JSON-RPC per the MCP stdio transport;
// stdout carries ONLY protocol frames (all logging goes to stderr); tool
// failures are results with isError=true (protocol errors are reserved for
// malformed requests / unknown methods / unknown tools).
package main

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/apache/arrow-go/v18/arrow/flight"
	"github.com/apache/arrow-go/v18/arrow/flight/flightsql"
)

const mcpProtocolVersion = "2025-06-18"
const mcpDefaultMaxRows = 200
const mcpHardMaxRows = 2000

func cmdMCP(args []string) error {
	fs := newFlagSet("mcp", `usage: sparrow mcp [-s profile] [flags]
Serve this CLI's core tools over the Model Context Protocol (stdio) — for
chat agents without a shell (Claude Desktop, claude.ai, Slack). Binds ONE
server (the profile/URI given here); the connection is dialed lazily on the
first tool call and kept warm across calls. Data tools: orient, sql (incl.
stats benchmark mode), pull, expect, verify. Diagnostics: doctor (+ server
conformance card), check, ping. Client-side: version, whatsnew, feedback
(these need no Flight server). Row output is capped (--max-rows, default 200,
hard cap 2000) so a result can't flood a model's context window. stdout
carries only protocol frames; logs go to stderr.
example (claude_desktop_config.json):
  { "mcpServers": { "sparrow": { "command": "sparrow", "args": ["mcp", "-s", "sparrow"] } } }`)
	cf := addConnFlags(fs)
	maxRows := fs.Int("max-rows", mcpDefaultMaxRows, "default row cap for sql/pull tool results (hard cap 2000)")
	parseFlags(fs, args)

	p, pname, err := cf.resolve()
	if err != nil {
		return err
	}
	srv := &mcpServer{profile: p, pname: pname, defaultCap: clampRows(*maxRows), connArgs: cf.asArgs()}
	fmt.Fprintf(os.Stderr, "sparrow mcp: serving %s (profile: %s) on stdio\n", p.URI, pname)
	defer srv.closeClient()
	return srv.serve(os.Stdin, os.Stdout)
}

func clampRows(n int) int {
	if n < 1 {
		return 1
	}
	if n > mcpHardMaxRows {
		return mcpHardMaxRows
	}
	return n
}

type mcpServer struct {
	profile    Profile
	pname      string
	defaultCap int
	clientName string   // from initialize clientInfo — attributes feedback reports
	connArgs   []string // the startup binding replayed as CLI flags, for captured commands

	mu      sync.Mutex // guards cl/clCtx (dial + drop)
	cl      *flightsql.Client
	clCtx   context.Context // carries the adopted auth metadata; NO deadline
	writeMu sync.Mutex      // one protocol frame at a time on stdout
}

// client returns the warm connection, dialing on first use. The stored
// context is deadline-free (a per-call timeout would kill the connection
// after it fired) — callers derive their own timeout from it so the auth
// metadata rides along.
func (s *mcpServer) client() (*flightsql.Client, context.Context, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.cl != nil {
		return s.cl, s.clCtx, nil
	}
	cl, cctx, err := dial(context.Background(), s.profile)
	if err != nil {
		return nil, nil, err
	}
	s.cl, s.clCtx = cl, cctx
	return cl, cctx, nil
}

func (s *mcpServer) closeClient() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.cl != nil {
		s.cl.Close()
		s.cl, s.clCtx = nil, nil
	}
}

// withClient runs fn with the warm client under a per-call timeout. On a
// connection-class failure it drops the cached client, re-dials once and
// retries — a long-lived server must survive a server restart between calls.
func (s *mcpServer) withClient(fn func(ctx context.Context, cl *flightsql.Client) error) error {
	run := func() error {
		cl, base, err := s.client()
		if err != nil {
			return connError{err}
		}
		ctx, cancel := context.WithTimeout(base, 2*time.Minute)
		defer cancel()
		return fn(ctx, cl)
	}
	err := run()
	if err == nil {
		return nil
	}
	var ce connError
	if errors.As(err, &ce) || errors.As(classifyConnErr(err, err), &ce) {
		s.closeClient()
		return run()
	}
	return err
}

// asArgs replays the parsed connection flags as CLI arguments, so a captured
// command (doctor/check/ping run in-process) targets the SAME binding the MCP
// server was started with.
func (cf *connFlags) asArgs() []string {
	var a []string
	if *cf.server != "" {
		a = append(a, "-s", *cf.server)
	}
	if *cf.basic != "" {
		a = append(a, "--basic", *cf.basic)
	}
	if *cf.bearer != "" {
		a = append(a, "--bearer", *cf.bearer)
	}
	for _, h := range cf.hdrs {
		a = append(a, "--header", h)
	}
	if *cf.tlsSkip {
		a = append(a, "--tls-skip-verify")
	}
	if *cf.cert != "" {
		a = append(a, "--tls-cert", *cf.cert)
	}
	if *cf.key != "" {
		a = append(a, "--tls-key", *cf.key)
	}
	if *cf.ca != "" {
		a = append(a, "--tls-ca", *cf.ca)
	}
	return a
}

// runCaptured executes a CLI command function in-process with real args,
// capturing everything it prints. Safe here because (a) the request loop is
// serial and (b) the protocol writer holds the ORIGINAL stdout *os.File —
// swapping the os.Stdout variable cannot touch frames already in flight.
// User-supplied values must be dash-guarded first: parseFlags os.Exit(3)s on
// an unknown flag, which would kill the whole server.
func runCaptured(fn func([]string) error, args []string) (stdout, stderr string, err error) {
	origOut, origErr := os.Stdout, os.Stderr
	ro, wo, e1 := os.Pipe()
	re, we, e2 := os.Pipe()
	if e1 != nil || e2 != nil {
		return "", "", fmt.Errorf("pipe: %v %v", e1, e2)
	}
	outC := make(chan string, 1)
	errC := make(chan string, 1)
	go func() { b, _ := io.ReadAll(ro); outC <- string(b) }()
	go func() { b, _ := io.ReadAll(re); errC <- string(b) }()
	os.Stdout, os.Stderr = wo, we
	defer func() {
		os.Stdout, os.Stderr = origOut, origErr
		wo.Close()
		we.Close()
		stdout, stderr = <-outC, <-errC
	}()
	err = fn(args)
	return
}

// capturedResult applies the expect/verify philosophy to captured commands:
// if the command produced its report, the report IS the answer — a doctor
// that found failures or a check with findings is a successful diagnosis,
// not a tool error (the command's gate verdict rides as a footer line). Only
// a command that produced NOTHING surfaces its error as isError.
func capturedResult(stdout string, err error) (string, error) {
	if strings.TrimSpace(stdout) != "" {
		if err != nil {
			return stdout + "\ngate: " + firstLine(err) + "\n", nil
		}
		return stdout, nil
	}
	return "", err
}

// dashGuard rejects user-supplied positional values that would be parsed as
// flags by the captured command (see runCaptured).
func dashGuard(vals ...string) error {
	for _, v := range vals {
		if strings.HasPrefix(strings.TrimSpace(v), "-") {
			return fmt.Errorf("value %q must not start with '-'", v)
		}
	}
	return nil
}

// ── JSON-RPC plumbing ────────────────────────────────────────────────────

type rpcRequest struct {
	Jsonrpc string          `json:"jsonrpc"`
	ID      json.RawMessage `json:"id"`
	Method  string          `json:"method"`
	Params  json.RawMessage `json:"params"`
}

func (s *mcpServer) writeFrame(w io.Writer, v any) {
	b, err := json.Marshal(v)
	if err != nil {
		return
	}
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	w.Write(append(b, '\n'))
}

func (s *mcpServer) writeResult(w io.Writer, id json.RawMessage, result any) {
	s.writeFrame(w, map[string]any{"jsonrpc": "2.0", "id": id, "result": result})
}

func (s *mcpServer) writeError(w io.Writer, id json.RawMessage, code int, msg string) {
	s.writeFrame(w, map[string]any{"jsonrpc": "2.0", "id": id,
		"error": map[string]any{"code": code, "message": msg}})
}

func (s *mcpServer) serve(r io.Reader, w io.Writer) error {
	sc := bufio.NewScanner(r)
	sc.Buffer(make([]byte, 0, 64*1024), 16*1024*1024) // SQL and receipts can be long
	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		if line == "" {
			continue
		}
		var req rpcRequest
		if err := json.Unmarshal([]byte(line), &req); err != nil {
			s.writeError(w, nil, -32700, "parse error: "+firstLine(err))
			continue
		}
		isNotification := len(req.ID) == 0 || string(req.ID) == "null"
		switch req.Method {
		case "initialize":
			var p struct {
				ProtocolVersion string `json:"protocolVersion"`
				ClientInfo      struct {
					Name string `json:"name"`
				} `json:"clientInfo"`
			}
			json.Unmarshal(req.Params, &p)
			pv := p.ProtocolVersion
			if pv == "" {
				pv = mcpProtocolVersion
			}
			s.clientName = strings.TrimSpace(p.ClientInfo.Name)
			s.writeResult(w, req.ID, map[string]any{
				"protocolVersion": pv,
				"capabilities":    map[string]any{"tools": map[string]any{}},
				"serverInfo": map[string]any{
					"name": "sparrow", "title": "Sparrow — Flight SQL",
					"version": version,
				},
				"instructions": "ONE bound Flight SQL server (" + s.profile.URI + "). Data tools: " +
					"start with orient (the map), then sql for queries (markdown table, row-capped; " +
					"stats:true = wire-anatomy benchmark), pull for 1-RTT ready tickets, expect to assert " +
					"a contract, verify to check a receipt. Diagnostics: doctor (layered connection " +
					"diagnosis; server_card:true = conformance card), check (data-quality findings), " +
					"ping (network vs server latency). Client-side: version, whatsnew (recent releases), " +
					"and feedback — reach the sparrow maintainers (no Flight server needed) when a tool " +
					"misbehaves or an idea comes up. Same verbs, same semantics as the sparrow CLI.",
			})
		case "ping":
			s.writeResult(w, req.ID, map[string]any{})
		case "tools/list":
			s.writeResult(w, req.ID, map[string]any{"tools": mcpToolDefs()})
		case "tools/call":
			var p struct {
				Name      string          `json:"name"`
				Arguments json.RawMessage `json:"arguments"`
			}
			if err := json.Unmarshal(req.Params, &p); err != nil {
				s.writeError(w, req.ID, -32602, "invalid params: "+firstLine(err))
				continue
			}
			text, toolErr, known := s.callTool(p.Name, p.Arguments)
			if !known {
				s.writeError(w, req.ID, -32602, "unknown tool: "+p.Name)
				continue
			}
			res := map[string]any{
				"content": []map[string]any{{"type": "text", "text": text}},
				"isError": toolErr,
			}
			s.writeResult(w, req.ID, res)
		default:
			if !isNotification { // unknown notifications are silently ignored per spec
				s.writeError(w, req.ID, -32601, "method not found: "+req.Method)
			}
		}
	}
	return sc.Err()
}

// ── the tool catalog (descriptions embed cmdDesc — pinned by mcp_test.go) ──

func mcpToolDefs() []map[string]any {
	obj := func(props map[string]any, required ...string) map[string]any {
		sch := map[string]any{"type": "object", "properties": props}
		if len(required) > 0 {
			sch["required"] = required
		}
		return sch
	}
	maxRowsProp := map[string]any{"type": "integer",
		"description": fmt.Sprintf("row cap for the result (default %d, hard cap %d)", mcpDefaultMaxRows, mcpHardMaxRows)}
	return []map[string]any{
		{
			"name":        "orient",
			"description": cmdDesc["orient"] + ". Call this FIRST on an unfamiliar server.",
			"inputSchema": obj(map[string]any{}),
		},
		{
			"name": "sql",
			"description": cmdDesc["sql"] + " and return the result as a markdown table (row-capped). " +
				"Read-only queries; the server may reject writes. With stats:true the result is instead the " +
				"query's ANATOMY — plan/first-byte/stream ms, wire bytes, codec + compression ratio, pacing — " +
				"the CLI's --stats benchmark (runs on its own fresh connection, full stream measured).",
			"inputSchema": obj(map[string]any{
				"query":    map[string]any{"type": "string", "description": "the SQL statement"},
				"max_rows": maxRowsProp,
				"stats":    map[string]any{"type": "boolean", "description": "return the wire anatomy instead of rows (benchmark mode)"},
			}, "query"),
		},
		{
			"name": "pull",
			"description": cmdDesc["pull"] + " — skips SQL planning entirely (1 round trip). " +
				`Sparrow-dialect JSON tickets: {"series": ["ID", ...], "start": "...", "end": "..."} or {"sql": "SELECT ..."}. ` +
				"lz4 compression is negotiated automatically. Servers that mint opaque tickets reject this — use sql there.",
			"inputSchema": obj(map[string]any{
				"ticket":   map[string]any{"type": "string", "description": "the ready ticket, as a JSON string"},
				"max_rows": maxRowsProp,
			}, "ticket"),
		},
		{
			"name": "expect",
			"description": cmdDesc["expect"] + ". Give at least one assertion; ALL must hold. " +
				"Scalar checks read the first cell; row counts are computed server-side (COUNT(*), never materialized); " +
				"cols checks the result's column names exactly, in order. A failed assertion is a normal result, not a tool error.",
			"inputSchema": obj(map[string]any{
				"query":    map[string]any{"type": "string", "description": "the SQL whose result is asserted"},
				"eq":       map[string]any{"type": "string", "description": "scalar equals (numeric when both parse, else string)"},
				"ne":       map[string]any{"type": "string", "description": "scalar not-equals"},
				"gt":       map[string]any{"type": "string", "description": "scalar greater-than"},
				"lt":       map[string]any{"type": "string", "description": "scalar less-than"},
				"ge":       map[string]any{"type": "string", "description": "scalar >="},
				"le":       map[string]any{"type": "string", "description": "scalar <="},
				"rows":     map[string]any{"type": "integer", "description": "exact row count"},
				"rows_min": map[string]any{"type": "integer", "description": "minimum row count"},
				"rows_max": map[string]any{"type": "integer", "description": "maximum row count"},
				"empty":    map[string]any{"type": "boolean", "description": "assert zero rows"},
				"nonempty": map[string]any{"type": "boolean", "description": "assert at least one row"},
				"cols":     map[string]any{"type": "array", "items": map[string]any{"type": "string"}, "description": "exact column names, in order"},
			}, "query"),
		},
		{
			"name": "verify",
			"description": cmdDesc["verify"] + " — against THIS server. Pass the receipt document (JSON) itself. " +
				"If the receipt names a different endpoint, the vendor mismatch is reported but the verdict gates on " +
				"DATA agreement (the CLI's `verify -s` semantics); on the receipt's own endpoint, server identity gates too.",
			"inputSchema": obj(map[string]any{
				"receipt": map[string]any{"type": "string", "description": "the receipt JSON document (contents of the .json file sparrow sql --receipt wrote)"},
			}, "receipt"),
		},
		{
			"name": "feedback",
			"description": cmdDesc["feedback"] + " — reaches them over HTTPS, independent of the Flight server " +
				"(works even when the server is down, which is exactly when you need it). " +
				"USE IT when a sparrow tool misbehaves or an idea comes up mid-session.",
			"inputSchema": obj(map[string]any{
				"message":  map[string]any{"type": "string", "description": "what happened, or the idea"},
				"category": map[string]any{"type": "string", "enum": []string{"bug", "idea", "general"}, "description": "defaults to general"},
				"from":     map[string]any{"type": "string", "description": "who this is from (defaults to the connected MCP client's name)"},
			}, "message"),
		},
		{
			"name": "doctor",
			"description": cmdDesc["doctor"] + ": config → dns → tcp → tls → auth → flightsql → roundtrip, " +
				"naming the layer that breaks — run this when a data tool reports the server unreachable. " +
				"With server_card:true it instead probes the server's Flight SQL CONFORMANCE surface " +
				"(catalog RPCs, prepared statements, Substrait, direct tickets, IPC compression). JSON report.",
			"inputSchema": obj(map[string]any{
				"server_card": map[string]any{"type": "boolean", "description": "probe the server's Flight SQL surface instead of the connection"},
			}),
		},
		{
			"name": "check",
			"description": cmdDesc["check"] + " — nulls, duplicate keys, staleness, frozen series, outliers; " +
				"server-side aggregates, JSON findings. ⚠ heavy on very large tables (full-table GROUP BYs): " +
				"prefer key columns with sane cardinality and avoid hammering small production servers.",
			"inputSchema": obj(map[string]any{
				"table":   map[string]any{"type": "string", "description": "the table to check"},
				"key":     map[string]any{"type": "string", "description": "key column(s), comma-separated — enables duplicate-key checks"},
				"time":    map[string]any{"type": "string", "description": "time column — enables staleness/frozen checks"},
				"max_age": map[string]any{"type": "string", "description": `staleness threshold, e.g. "7d"`},
				"fail_on": map[string]any{"type": "string", "description": "gate only these named checks (comma list); the rest still report"},
			}, "table"),
		},
		{
			"name": "ping",
			"description": cmdDesc["ping"] + " — separates network latency from server time " +
				"(bare TCP median vs warm-RPC median). JSON percentiles.",
			"inputSchema": obj(map[string]any{
				"n": map[string]any{"type": "integer", "description": "rounds (default 5)"},
			}),
		},
		{
			"name":        "version",
			"description": cmdDesc["version"] + " of this sparrow MCP server, and which Flight SQL endpoint it is bound to.",
			"inputSchema": obj(map[string]any{}),
		},
		{
			"name": "whatsnew",
			"description": cmdDesc["whatsnew"] + " — the notes are generated from the shipped commits, " +
				"so this is always the released truth, not a maintained changelog.",
			"inputSchema": obj(map[string]any{
				"n": map[string]any{"type": "integer", "description": "how many releases to show (default 3, max 20)"},
			}),
		},
	}
}

// callTool dispatches one tools/call. Returns (text, isError, knownTool).
func (s *mcpServer) callTool(name string, args json.RawMessage) (string, bool, bool) {
	var text string
	var err error
	switch name {
	case "orient":
		err = s.withClient(func(ctx context.Context, cl *flightsql.Client) error {
			var e error
			text, e = orientMarkdown(ctx, cl, s.profile.URI, s.pname)
			return e
		})
	case "sql":
		text, err = s.toolSQL(args)
	case "pull":
		text, err = s.toolPull(args)
	case "expect":
		text, err = s.toolExpect(args)
	case "verify":
		text, err = s.toolVerify(args)
	case "doctor":
		var a struct {
			ServerCard bool `json:"server_card"`
		}
		json.Unmarshal(args, &a)
		cargs := append(append([]string{}, s.connArgs...), "-o", "json")
		if a.ServerCard {
			cargs = append(cargs, "--server")
		}
		stdout, _, cerr := runCaptured(cmdDoctor, cargs)
		text, err = capturedResult(stdout, cerr)
	case "check":
		var a struct {
			Table  string `json:"table"`
			Key    string `json:"key"`
			Time   string `json:"time"`
			MaxAge string `json:"max_age"`
			FailOn string `json:"fail_on"`
		}
		if json.Unmarshal(args, &a) != nil || strings.TrimSpace(a.Table) == "" {
			return `check needs {"table": "..."}`, true, true
		}
		if gerr := dashGuard(a.Table, a.Key, a.Time, a.MaxAge, a.FailOn); gerr != nil {
			return firstLine(gerr), true, true
		}
		cargs := append(append([]string{}, s.connArgs...), a.Table, "-o", "json")
		for _, f := range []struct{ flag, v string }{
			{"key", a.Key}, {"time", a.Time}, {"max-age", a.MaxAge}, {"fail-on", a.FailOn}} {
			if strings.TrimSpace(f.v) != "" {
				cargs = append(cargs, "--"+f.flag, f.v)
			}
		}
		stdout, _, cerr := runCaptured(cmdCheck, cargs)
		text, err = capturedResult(stdout, cerr)
	case "ping":
		var a struct {
			N int `json:"n"`
		}
		json.Unmarshal(args, &a)
		cargs := append(append([]string{}, s.connArgs...), "-o", "json")
		if a.N > 0 {
			cargs = append(cargs, "-n", strconv.Itoa(a.N))
		}
		stdout, _, cerr := runCaptured(cmdPing, cargs)
		text, err = capturedResult(stdout, cerr)
	case "feedback":
		text, err = s.toolFeedback(args)
	case "version":
		text = fmt.Sprintf("sparrow %s · bound to %s (profile: %s) · MCP stdio", versionString(), s.profile.URI, s.pname)
	case "whatsnew":
		var a struct {
			N int `json:"n"`
		}
		json.Unmarshal(args, &a)
		if a.N == 0 {
			a.N = 3
		}
		text, err = whatsnewMarkdown(a.N)
	default:
		return "", false, false
	}
	if err != nil {
		return firstLine(err), true, true
	}
	return text, false, true
}

// ── tools ────────────────────────────────────────────────────────────────

func (s *mcpServer) toolSQL(raw json.RawMessage) (string, error) {
	var a struct {
		Query   string `json:"query"`
		MaxRows int    `json:"max_rows"`
		Stats   bool   `json:"stats"`
	}
	if err := json.Unmarshal(raw, &a); err != nil || strings.TrimSpace(a.Query) == "" {
		return "", fmt.Errorf(`sql needs {"query": "SELECT ..."}`)
	}
	if a.Stats {
		// benchmark mode: the CLI's --stats anatomy, on its own fresh
		// connection with the wire counter attached; the data itself goes to
		// a temp file and is discarded — the anatomy (stderr) is the answer
		if err := dashGuard(a.Query); err != nil {
			return "", err
		}
		tmp, err := os.CreateTemp("", "sparrow-mcp-*.csv")
		if err != nil {
			return "", err
		}
		tmp.Close()
		defer os.Remove(tmp.Name())
		cargs := append(append([]string{}, s.connArgs...), a.Query, "--stats", "-o", tmp.Name())
		_, stderrTxt, cerr := runCaptured(cmdSQL, cargs)
		return capturedResult(stderrTxt, cerr)
	}
	limit := s.defaultCap
	if a.MaxRows > 0 {
		limit = clampRows(a.MaxRows)
	}
	var out string
	err := s.withClient(func(ctx context.Context, cl *flightsql.Client) error {
		cols, err := querySchemaCols(ctx, cl, strings.TrimRight(a.Query, "; \t\r\n"))
		if err != nil {
			return err
		}
		rows, err := queryRows(ctx, cl, a.Query, limit+1)
		if err != nil {
			return err
		}
		out = mcpTable(cols, rows, limit)
		return nil
	})
	return out, err
}

func (s *mcpServer) toolPull(raw json.RawMessage) (string, error) {
	var a struct {
		Ticket  string `json:"ticket"`
		MaxRows int    `json:"max_rows"`
	}
	if err := json.Unmarshal(raw, &a); err != nil || strings.TrimSpace(a.Ticket) == "" {
		return "", fmt.Errorf(`pull needs {"ticket": "{\"series\": [...]}"}`)
	}
	limit := s.defaultCap
	if a.MaxRows > 0 {
		limit = clampRows(a.MaxRows)
	}
	ticket := withAcceptCompression([]byte(strings.TrimSpace(a.Ticket)), "lz4")
	var out string
	err := s.withClient(func(ctx context.Context, cl *flightsql.Client) error {
		rdr, err := cl.DoGet(ctx, &flight.Ticket{Ticket: ticket})
		if err != nil {
			return err
		}
		defer rdr.Release()
		var cols []string
		var rows [][]string
		for rdr.Next() && len(rows) <= limit {
			rec := rdr.Record()
			if cols == nil {
				for _, f := range rec.Schema().Fields() {
					cols = append(cols, f.Name)
				}
			}
			for r := 0; r < int(rec.NumRows()) && len(rows) <= limit; r++ {
				row := make([]string, rec.NumCols())
				for c := range row {
					row[c] = cell(rec.Column(c), r)
				}
				rows = append(rows, row)
			}
		}
		if err := rdr.Err(); err != nil && err != io.EOF {
			return err
		}
		if cols == nil {
			if sc := rdr.Schema(); sc != nil {
				for _, f := range sc.Fields() {
					cols = append(cols, f.Name)
				}
			}
		}
		out = mcpTable(cols, rows, limit)
		return nil
	})
	return out, err
}

func (s *mcpServer) toolExpect(raw json.RawMessage) (string, error) {
	var a struct {
		Query    string   `json:"query"`
		Eq       *string  `json:"eq"`
		Ne       *string  `json:"ne"`
		Gt       *string  `json:"gt"`
		Lt       *string  `json:"lt"`
		Ge       *string  `json:"ge"`
		Le       *string  `json:"le"`
		Rows     *int     `json:"rows"`
		RowsMin  *int     `json:"rows_min"`
		RowsMax  *int     `json:"rows_max"`
		Empty    bool     `json:"empty"`
		Nonempty bool     `json:"nonempty"`
		Cols     []string `json:"cols"`
	}
	if err := json.Unmarshal(raw, &a); err != nil || strings.TrimSpace(a.Query) == "" {
		return "", fmt.Errorf(`expect needs {"query": "...", ...at least one assertion}`)
	}
	inner := strings.TrimRight(strings.TrimSpace(a.Query), "; \t\r\n")
	scalars := []struct {
		v  *string
		k  string
		op string
	}{{a.Eq, "eq", "=="}, {a.Ne, "ne", "!="}, {a.Gt, "gt", ">"}, {a.Lt, "lt", "<"}, {a.Ge, "ge", ">="}, {a.Le, "le", "<="}}
	needScalar := false
	for _, sc := range scalars {
		if sc.v != nil {
			needScalar = true
		}
	}
	needCount := a.Rows != nil || a.RowsMin != nil || a.RowsMax != nil || a.Empty || a.Nonempty
	needCols := len(a.Cols) > 0
	if !needScalar && !needCount && !needCols {
		return "", fmt.Errorf("expect: give at least one assertion (eq/rows/empty/cols/…)")
	}

	var results []expectResult
	add := func(assertion, expected, actual string, pass bool) {
		results = append(results, expectResult{assertion, expected, actual, pass})
	}
	err := s.withClient(func(ctx context.Context, cl *flightsql.Client) error {
		if needScalar {
			row, err := queryRow(ctx, cl, inner)
			if err != nil {
				return err
			}
			for _, sc := range scalars {
				if sc.v == nil {
					continue
				}
				if len(row) == 0 {
					add(sc.k+" "+*sc.v, *sc.v, "<no rows>", false)
				} else {
					add(fmt.Sprintf("scalar %s %s", sc.op, *sc.v), *sc.v, row[0], compareScalar(row[0], *sc.v, sc.k))
				}
			}
		}
		if needCount {
			row, err := queryRow(ctx, cl, "SELECT count(*) FROM ("+inner+") AS __expect")
			if err != nil {
				return err
			}
			n := 0
			if len(row) > 0 {
				n, _ = strconv.Atoi(strings.TrimSpace(row[0]))
			}
			got := strconv.Itoa(n)
			if a.Rows != nil {
				add(fmt.Sprintf("rows == %d", *a.Rows), strconv.Itoa(*a.Rows), got, n == *a.Rows)
			}
			if a.RowsMin != nil {
				add(fmt.Sprintf("rows >= %d", *a.RowsMin), strconv.Itoa(*a.RowsMin), got, n >= *a.RowsMin)
			}
			if a.RowsMax != nil {
				add(fmt.Sprintf("rows <= %d", *a.RowsMax), strconv.Itoa(*a.RowsMax), got, n <= *a.RowsMax)
			}
			if a.Empty {
				add("rows == 0 (empty)", "0", got, n == 0)
			}
			if a.Nonempty {
				add("rows >= 1 (nonempty)", "1", got, n >= 1)
			}
		}
		if needCols {
			names, err := querySchemaCols(ctx, cl, inner)
			if err != nil {
				return err
			}
			add("cols == "+strings.Join(a.Cols, ","), strings.Join(a.Cols, ","),
				strings.Join(names, ","), equalStrings(a.Cols, names))
		}
		return nil
	})
	if err != nil {
		return "", err
	}
	var b strings.Builder
	fails := 0
	for _, r := range results {
		mark := "✓"
		if !r.Pass {
			mark, fails = "✗", fails+1
		}
		fmt.Fprintf(&b, "%s %s", mark, r.Assertion)
		if !r.Pass {
			fmt.Fprintf(&b, "  (got %s)", r.Actual)
		}
		b.WriteString("\n")
	}
	if fails > 0 {
		fmt.Fprintf(&b, "FAILED — %d of %d assertion(s) did not hold\n", fails, len(results))
	} else {
		fmt.Fprintf(&b, "ok — all %d assertion(s) hold\n", len(results))
	}
	return b.String(), nil
}

func (s *mcpServer) toolVerify(raw json.RawMessage) (string, error) {
	var a struct {
		Receipt string `json:"receipt"`
	}
	if err := json.Unmarshal(raw, &a); err != nil || strings.TrimSpace(a.Receipt) == "" {
		return "", fmt.Errorf(`verify needs {"receipt": "<the receipt JSON document>"}`)
	}
	var r receipt
	if err := json.Unmarshal([]byte(a.Receipt), &r); err != nil {
		return "", fmt.Errorf("not a sparrow receipt (bad JSON): %s", firstLine(err))
	}
	if r.Kind == "" || r.Query == "" {
		return "", fmt.Errorf("not a sparrow receipt (missing sparrow_receipt/query)")
	}
	if r.Result.DigestSum == "" || r.Result.DigestXor == "" {
		return "", fmt.Errorf("incomplete receipt (missing result fingerprint)")
	}
	if r.Algo != "" && r.Algo != receiptAlgo {
		return "", fmt.Errorf("receipt algo %q is not supported by this binary (%s)", r.Algo, receiptAlgo)
	}
	// The MCP server is BOUND to one endpoint. On the receipt's own endpoint
	// this is a bare verify (identity gates); on any other it has `verify -s`
	// semantics (vendor reported, data agreement gates) — same rules as the CLI.
	usedOverride := r.Server.Endpoint != s.profile.URI
	var out string
	err := s.withClient(func(ctx context.Context, cl *flightsql.Client) error {
		nowVendor := probeVendor(ctx, cl)
		res, ferr := fingerprint(ctx, cl, r.Query)
		if ferr != nil {
			return ferr
		}
		rowsMatch := res.Rows == r.Result.Rows
		digestMatch := res.DigestSum == r.Result.DigestSum && res.DigestXor == r.Result.DigestXor
		colsMatch := len(r.Result.Columns) == 0 || equalStrings(r.Result.Columns, res.Columns)
		vendorMatch := nowVendor == r.Server.Vendor
		dataOK := rowsMatch && digestMatch && colsMatch
		ok := dataOK && (usedOverride || vendorMatch)
		mark := func(b bool) string {
			if b {
				return "✓"
			}
			return "✗"
		}
		var b strings.Builder
		fmt.Fprintf(&b, "verify against %s%s\n", s.profile.URI,
			map[bool]string{true: " (differs from the receipt's endpoint — data agreement gates)", false: " (the receipt's own endpoint)"}[usedOverride])
		fmt.Fprintf(&b, " %s server    %s%s\n", mark(vendorMatch || usedOverride), nowVendor, mismatchNote(vendorMatch, r.Server.Vendor))
		fmt.Fprintf(&b, " %s rows      %s%s\n", mark(rowsMatch), groupDigits(strconv.FormatInt(res.Rows, 10)),
			mismatchNote(rowsMatch, groupDigits(strconv.FormatInt(r.Result.Rows, 10))))
		fmt.Fprintf(&b, " %s columns   %s\n", mark(colsMatch),
			map[bool]string{true: strings.Join(res.Columns, ", "), false: "DIFFER from the receipt"}[colsMatch])
		fmt.Fprintf(&b, " %s digest    %s\n", mark(digestMatch),
			map[bool]string{true: "matches", false: "DIFFERS — the result changed since the receipt"}[digestMatch])
		if ok {
			b.WriteString("VERIFIED — the receipt reproduces\n")
		} else {
			fmt.Fprintf(&b, "FAILED — %s\n", failReason(dataOK, vendorMatch, usedOverride))
		}
		out = b.String()
		return nil
	})
	return out, err
}

func (s *mcpServer) toolFeedback(raw json.RawMessage) (string, error) {
	var a struct {
		Message  string `json:"message"`
		Category string `json:"category"`
		From     string `json:"from"`
	}
	if err := json.Unmarshal(raw, &a); err != nil || strings.TrimSpace(a.Message) == "" {
		return "", fmt.Errorf(`feedback needs {"message": "..."}`)
	}
	if a.Category == "" {
		a.Category = "general"
	}
	user := strings.TrimSpace(a.From)
	if user == "" {
		user = s.clientName
	}
	if user == "" {
		user = "mcp-agent"
	}
	url := os.Getenv("SPARROW_FEEDBACK_URL")
	if url == "" {
		url = feedbackURL
	}
	ts, err := sendFeedback(url, strings.TrimSpace(a.Message), a.Category, user, s.profile.URI)
	if err != nil {
		return "", err
	}
	return "feedback delivered (" + ts + ") — thank you\n", nil
}

// mcpTable renders rows as a markdown table, capped; fetch limit+1 rows so
// truncation is announced honestly instead of masquerading as completeness.
func mcpTable(cols []string, rows [][]string, limit int) string {
	esc := func(s string) string {
		s = strings.ReplaceAll(s, "|", "\\|")
		return strings.ReplaceAll(s, "\n", " ")
	}
	var b strings.Builder
	if len(cols) == 0 {
		return "(no result schema)\n"
	}
	for i, c := range cols {
		if i > 0 {
			b.WriteString(" | ")
		}
		b.WriteString(esc(c))
	}
	b.WriteString("\n")
	for i := range cols {
		if i > 0 {
			b.WriteString(" | ")
		}
		b.WriteString("---")
	}
	b.WriteString("\n")
	truncated := len(rows) > limit
	if truncated {
		rows = rows[:limit]
	}
	for _, row := range rows {
		for i, v := range row {
			if i > 0 {
				b.WriteString(" | ")
			}
			b.WriteString(esc(v))
		}
		b.WriteString("\n")
	}
	fmt.Fprintf(&b, "\n%s row(s)", groupDigits(strconv.Itoa(len(rows))))
	if truncated {
		fmt.Fprintf(&b, " — TRUNCATED at the %d-row cap (raise max_rows, or aggregate/filter server-side)", limit)
	}
	b.WriteString("\n")
	return b.String()
}
