// sparrow — a terminal client for any Arrow Flight / Flight SQL server.
// M0: connect · ls · sql · TTY table / Arrow IPC pipe output.
package main

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/csv"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/flight"
	"github.com/apache/arrow-go/v18/arrow/flight/flightsql"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/compress"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

// version is stamped by goreleaser (-X main.version={{.Version}}) on releases
var version = "0.4.0-dev"

// ── profiles ────────────────────────────────────────────────────────────

type Profile struct {
	URI           string            `json:"uri"`
	Auth          string            `json:"auth"` // "basic" | "bearer" | "none"
	User          string            `json:"user,omitempty"`
	Pass          string            `json:"pass,omitempty"`
	Token         string            `json:"token,omitempty"`   // bearer auth (InfluxDB 3 style)
	Headers       map[string]string `json:"headers,omitempty"` // extra per-call metadata (e.g. database: mydb)
	TLSSkipVerify bool              `json:"tls_skip_verify,omitempty"`
	TLSCert       string            `json:"tls_cert,omitempty"` // client certificate (mTLS), PEM path
	TLSKey        string            `json:"tls_key,omitempty"`  // client private key (mTLS), PEM path
	TLSCA         string            `json:"tls_ca,omitempty"`   // CA bundle to verify the server, PEM path
}

type Config struct {
	Default  string             `json:"default"`
	Profiles map[string]Profile `json:"profiles"`
}

func configPath() string {
	home, _ := os.UserHomeDir()
	return filepath.Join(home, ".sparrow", "config.json")
}

func loadConfig() Config {
	cfg := Config{Profiles: map[string]Profile{}}
	b, err := os.ReadFile(configPath())
	if err == nil {
		_ = json.Unmarshal(b, &cfg)
	}
	if cfg.Profiles == nil {
		cfg.Profiles = map[string]Profile{}
	}
	return cfg
}

func saveConfig(cfg Config) error {
	if err := os.MkdirAll(filepath.Dir(configPath()), 0o700); err != nil {
		return err
	}
	b, _ := json.MarshalIndent(cfg, "", "  ")
	return os.WriteFile(configPath(), b, 0o600)
}

// ── connection ──────────────────────────────────────────────────────────

// connError marks connection/auth failures so main can exit 2 (vs 1 for
// query errors) — lets scripts and agents branch on the failure class.
type connError struct{ err error }

func (e connError) Error() string { return e.err.Error() }
func (e connError) Unwrap() error { return e.err }

func parseURI(uri string) (target string, useTLS bool, err error) {
	switch {
	case strings.HasPrefix(uri, "grpc+tls://"):
		return strings.TrimPrefix(uri, "grpc+tls://"), true, nil
	case strings.HasPrefix(uri, "grpcs://"):
		return strings.TrimPrefix(uri, "grpcs://"), true, nil
	case strings.HasPrefix(uri, "grpc://"):
		return strings.TrimPrefix(uri, "grpc://"), false, nil
	default:
		return "", false, fmt.Errorf("URI must start with grpc:// or grpc+tls:// (got %q)", uri)
	}
}

func dial(ctx context.Context, p Profile) (*flightsql.Client, context.Context, error) {
	target, useTLS, err := parseURI(p.URI)
	if err != nil {
		return nil, ctx, connError{err}
	}
	var creds grpc.DialOption
	if useTLS {
		tc := &tls.Config{
			InsecureSkipVerify: p.TLSSkipVerify, // GizmoSQL ships self-signed by default
		}
		if p.TLSCert != "" || p.TLSKey != "" {
			pair, err := tls.LoadX509KeyPair(p.TLSCert, p.TLSKey)
			if err != nil {
				return nil, ctx, connError{fmt.Errorf("mTLS keypair: %w", err)}
			}
			tc.Certificates = []tls.Certificate{pair}
		}
		if p.TLSCA != "" {
			pem, err := os.ReadFile(p.TLSCA)
			if err != nil {
				return nil, ctx, connError{fmt.Errorf("tls-ca: %w", err)}
			}
			pool := x509.NewCertPool()
			if !pool.AppendCertsFromPEM(pem) {
				return nil, ctx, connError{fmt.Errorf("tls-ca: no PEM certificates in %s", p.TLSCA)}
			}
			tc.RootCAs = pool
		}
		creds = grpc.WithTransportCredentials(credentials.NewTLS(tc))
	} else {
		creds = grpc.WithTransportCredentials(insecure.NewCredentials())
	}
	cl, err := flightsql.NewClient(target, nil, nil, creds)
	if err != nil {
		return nil, ctx, connError{err}
	}
	// extra per-call headers ride the outgoing metadata (InfluxDB 3 needs
	// `database: <db>` on every call)
	if len(p.Headers) > 0 {
		kv := make([]string, 0, len(p.Headers)*2)
		for k, v := range p.Headers {
			kv = append(kv, k, v)
		}
		ctx = metadata.AppendToOutgoingContext(ctx, kv...)
	}
	switch p.Auth {
	case "basic":
		authCtx, err := cl.Client.AuthenticateBasicToken(ctx, p.User, p.Pass)
		if err != nil {
			cl.Close()
			return nil, ctx, connError{fmt.Errorf("auth failed: %w", err)}
		}
		return cl, authCtx, nil
	case "bearer":
		ctx = metadata.AppendToOutgoingContext(ctx, "authorization", "Bearer "+p.Token)
	}
	return cl, ctx, nil
}

// multiFlag collects a repeatable --header k=v flag.
type multiFlag []string

func (m *multiFlag) String() string     { return strings.Join(*m, ",") }
func (m *multiFlag) Set(v string) error { *m = append(*m, v); return nil }

func parseHeaders(hs []string) (map[string]string, error) {
	if len(hs) == 0 {
		return nil, nil
	}
	out := map[string]string{}
	for _, h := range hs {
		k, v, ok := strings.Cut(h, "=")
		if !ok {
			return nil, fmt.Errorf("--header wants key=value (got %q)", h)
		}
		out[strings.TrimSpace(k)] = strings.TrimSpace(v)
	}
	return out, nil
}

// connFlags registers the connection flags shared by every data command and
// resolves them to a profile (saved profile via -s, or an ad-hoc URI).
type connFlags struct {
	server, basic, bearer *string
	cert, key, ca         *string
	tlsSkip               *bool
	hdrs                  multiFlag
}

func addConnFlags(fs *flag.FlagSet) *connFlags {
	cf := &connFlags{}
	cf.server = fs.String("s", "", "profile name or grpc URI")
	cf.basic = fs.String("basic", "", "user:pass for ad-hoc URIs")
	cf.bearer = fs.String("bearer", "", "bearer token for ad-hoc URIs")
	fs.Var(&cf.hdrs, "header", "extra per-call metadata key=value (repeatable)")
	cf.tlsSkip = fs.Bool("tls-skip-verify", false, "accept self-signed TLS certs")
	cf.cert = fs.String("tls-cert", "", "client certificate for mTLS (PEM path)")
	cf.key = fs.String("tls-key", "", "client private key for mTLS (PEM path)")
	cf.ca = fs.String("tls-ca", "", "CA bundle to verify the server (PEM path)")
	return cf
}

func (cf *connFlags) resolve() (Profile, string, error) {
	adhoc := Profile{
		Auth: "none", TLSSkipVerify: *cf.tlsSkip,
		TLSCert: *cf.cert, TLSKey: *cf.key, TLSCA: *cf.ca,
	}
	if *cf.basic != "" {
		u, pw, _ := strings.Cut(*cf.basic, ":")
		adhoc.Auth, adhoc.User, adhoc.Pass = "basic", u, pw
	}
	if *cf.bearer != "" {
		adhoc.Auth, adhoc.Token = "bearer", *cf.bearer
	}
	h, err := parseHeaders(cf.hdrs)
	if err != nil {
		return Profile{}, "", err
	}
	adhoc.Headers = h
	return resolveProfile(*cf.server, adhoc)
}

// resolveProfile picks the connection: -s <profile|uri>, else the default profile.
func resolveProfile(server string, adhoc Profile) (Profile, string, error) {
	cfg := loadConfig()
	name := server
	if name == "" {
		name = cfg.Default
	}
	if p, ok := cfg.Profiles[name]; ok && name != "" {
		return p, name, nil
	}
	if strings.Contains(server, "://") { // ad-hoc URI
		adhoc.URI = server
		return adhoc, "(ad-hoc)", nil
	}
	if server == "" && cfg.Default == "" {
		return Profile{}, "", connError{fmt.Errorf("no default profile — run: sparrow connect <uri> [--basic user:pass]")}
	}
	return Profile{}, "", connError{fmt.Errorf("unknown profile %q (see: sparrow profiles)", name)}
}

// ── output ──────────────────────────────────────────────────────────────

func stdoutIsTTY() bool {
	fi, err := os.Stdout.Stat()
	return err == nil && fi.Mode()&os.ModeCharDevice != 0
}

func cell(col arrow.Array, row int) string {
	if col.IsNull(row) {
		return "NULL"
	}
	// dense-union values (e.g. GetSqlInfo) render as [typeid,"value"] via
	// ValueStr — unwrap to the active child instead
	switch u := col.(type) {
	case *array.DenseUnion:
		return cell(u.Field(u.ChildID(row)), int(u.ValueOffset(row)))
	case *array.SparseUnion:
		return cell(u.Field(u.ChildID(row)), row)
	}
	return col.ValueStr(row)
}

var stdoutFormats = map[string]bool{
	"table": true, "csv": true, "json": true, "jsonl": true, "md": true, "arrow": true,
}

type sink struct {
	format string
	w      io.Writer
	closer io.Closer // file to close, nil for stdout
	path   string    // file path, "" for stdout
	encKey []byte    // parquet only: Parquet Modular Encryption footer key
}

// loadKey parses --encrypt-key: a hex string, env:VAR (hex), or file:path
// (hex text or raw bytes). AES wants 16, 24 or 32 bytes.
func loadKey(spec string) ([]byte, error) {
	hexStr := spec
	switch {
	case strings.HasPrefix(spec, "env:"):
		hexStr = os.Getenv(spec[4:])
		if hexStr == "" {
			return nil, fmt.Errorf("--encrypt-key: environment variable %s is empty", spec[4:])
		}
	case strings.HasPrefix(spec, "file:"):
		b, err := os.ReadFile(spec[5:])
		if err != nil {
			return nil, fmt.Errorf("--encrypt-key: %w", err)
		}
		switch len(b) {
		case 16, 24, 32: // raw key bytes
			return b, nil
		}
		hexStr = strings.TrimSpace(string(b))
	}
	key, err := hex.DecodeString(strings.TrimSpace(hexStr))
	if err != nil {
		return nil, fmt.Errorf("--encrypt-key: not valid hex (use hex, env:VAR or file:path): %w", err)
	}
	switch len(key) {
	case 16, 24, 32:
		return key, nil
	}
	return nil, fmt.Errorf("--encrypt-key: AES wants 16, 24 or 32 bytes (got %d)", len(key))
}

// resolveSink maps -o to a format + destination. Bare format name → stdout;
// anything with an extension → file, format inferred. Empty → TTY table /
// pipe Arrow IPC (the M0 behavior, unchanged).
func resolveSink(o string) (sink, error) {
	if o == "" {
		if stdoutIsTTY() {
			return sink{format: "table", w: os.Stdout}, nil
		}
		return sink{format: "arrow", w: os.Stdout}, nil
	}
	name := strings.ToLower(o)
	if name == "markdown" {
		name = "md"
	}
	if stdoutFormats[name] && !strings.ContainsAny(o, "./\\") {
		return sink{format: name, w: os.Stdout}, nil
	}
	var format string
	switch strings.ToLower(strings.TrimPrefix(filepath.Ext(o), ".")) {
	case "arrow", "arrows", "ipc":
		format = "arrow"
	case "parquet", "pq":
		format = "parquet"
	case "csv":
		format = "csv"
	case "json":
		format = "json"
	case "jsonl", "ndjson":
		format = "jsonl"
	case "md":
		format = "md"
	default:
		return sink{}, fmt.Errorf("-o: unknown format or extension %q (formats: table csv json jsonl md arrow · files: .arrow .parquet .csv .json .jsonl .md)", o)
	}
	f, err := os.Create(o)
	if err != nil {
		return sink{}, err
	}
	return sink{format: format, w: f, closer: f, path: o}, nil
}

func mdEscape(vals []string) []string {
	out := make([]string, len(vals))
	for i, v := range vals {
		out[i] = strings.NewReplacer("|", "\\|", "\n", " ").Replace(v)
	}
	return out
}

// recordWriter streams flight record batches into one output format.
type recordWriter struct {
	s       sink
	maxRows int   // >0 caps emitted rows; the stream is still drained for the total
	total   int64 // rows received
	written int64 // rows emitted
	schema  *arrow.Schema
	tw      *tabwriter.Writer
	cw      *csv.Writer
	iw      *ipc.Writer
	pw      *pqarrow.FileWriter
	jsonAny bool
}

func (rw *recordWriter) begin(schema *arrow.Schema) error {
	rw.schema = schema
	heads := make([]string, len(schema.Fields()))
	for i, f := range schema.Fields() {
		heads[i] = f.Name
	}
	switch rw.s.format {
	case "table":
		rw.tw = tabwriter.NewWriter(rw.s.w, 2, 4, 2, ' ', 0)
		fmt.Fprintln(rw.tw, strings.Join(heads, "\t"))
	case "md":
		fmt.Fprintln(rw.s.w, "| "+strings.Join(mdEscape(heads), " | ")+" |")
		sep := make([]string, len(heads))
		for i := range sep {
			sep[i] = "---"
		}
		fmt.Fprintln(rw.s.w, "| "+strings.Join(sep, " | ")+" |")
	case "csv":
		rw.cw = csv.NewWriter(rw.s.w)
		if err := rw.cw.Write(heads); err != nil {
			return err
		}
	case "json":
		fmt.Fprint(rw.s.w, "[")
	case "jsonl":
	case "arrow":
		rw.iw = ipc.NewWriter(rw.s.w, ipc.WithSchema(schema))
	case "parquet":
		opts := []parquet.WriterProperty{parquet.WithCompression(compress.Codecs.Snappy)}
		if len(rw.s.encKey) > 0 {
			// Parquet Modular Encryption (AES-GCM, encrypted footer) — the
			// in-spec, cross-tool format; DuckDB/Spark/pyarrow read it back
			opts = append(opts, parquet.WithEncryptionProperties(
				parquet.NewFileEncryptionProperties(string(rw.s.encKey))))
		}
		props := parquet.NewWriterProperties(opts...)
		var err error
		rw.pw, err = pqarrow.NewFileWriter(schema, rw.s.w, props, pqarrow.DefaultWriterProps())
		if err != nil {
			return err
		}
	}
	return nil
}

func (rw *recordWriter) write(rec arrow.Record) error {
	rw.total += rec.NumRows()
	emit := rec
	if rw.maxRows > 0 {
		remain := int64(rw.maxRows) - rw.written
		if remain <= 0 {
			return nil // keep draining for the total count
		}
		if rec.NumRows() > remain {
			emit = rec.NewSlice(0, remain)
			defer emit.Release()
		}
	}
	n := emit.NumRows()
	switch rw.s.format {
	case "table", "md", "csv":
		for r := 0; r < int(n); r++ {
			vals := make([]string, int(emit.NumCols()))
			for c := range vals {
				if rw.s.format == "csv" && emit.Column(c).IsNull(r) {
					vals[c] = "" // empty cell = NA for pandas/Excel; "NULL" would arrive as a string
					continue
				}
				vals[c] = cell(emit.Column(c), r)
			}
			switch rw.s.format {
			case "table":
				fmt.Fprintln(rw.tw, strings.Join(vals, "\t"))
			case "md":
				fmt.Fprintln(rw.s.w, "| "+strings.Join(mdEscape(vals), " | ")+" |")
			case "csv":
				if err := rw.cw.Write(vals); err != nil {
					return err
				}
			}
		}
	case "json", "jsonl":
		var buf bytes.Buffer
		if err := array.RecordToJSON(emit, &buf); err != nil {
			return err
		}
		for _, ln := range strings.Split(strings.TrimRight(buf.String(), "\n"), "\n") {
			if ln == "" {
				continue
			}
			if rw.s.format == "jsonl" {
				fmt.Fprintln(rw.s.w, ln)
			} else {
				if rw.jsonAny {
					fmt.Fprint(rw.s.w, ",")
				}
				fmt.Fprint(rw.s.w, "\n  "+ln)
				rw.jsonAny = true
			}
		}
	case "arrow":
		if err := rw.iw.Write(emit); err != nil {
			return err
		}
	case "parquet":
		if err := rw.pw.Write(emit); err != nil {
			return err
		}
	}
	rw.written += n
	return nil
}

func (rw *recordWriter) end() error {
	if rw.schema == nil { // no data ever arrived
		if rw.s.format == "json" {
			fmt.Fprintln(rw.s.w, "[]")
		}
		return nil
	}
	switch rw.s.format {
	case "table":
		return rw.tw.Flush()
	case "csv":
		rw.cw.Flush()
		return rw.cw.Error()
	case "json":
		if rw.jsonAny {
			fmt.Fprintln(rw.s.w, "\n]")
		} else {
			fmt.Fprintln(rw.s.w, "]")
		}
	case "arrow":
		return rw.iw.Close()
	case "parquet":
		return rw.pw.Close()
	}
	return nil
}

func consumeInfo(ctx context.Context, cl *flightsql.Client, info *flight.FlightInfo, s sink, maxRows int) (int64, error) {
	rw := &recordWriter{s: s, maxRows: maxRows}
	for _, ep := range info.Endpoint {
		rdr, err := cl.DoGet(ctx, ep.Ticket)
		if err != nil {
			return rw.total, err
		}
		if rw.schema == nil {
			if err := rw.begin(rdr.Schema()); err != nil {
				rdr.Release()
				return rw.total, err
			}
		}
		for rdr.Next() {
			if err := rw.write(rdr.Record()); err != nil {
				rdr.Release()
				return rw.total, err
			}
		}
		err = rdr.Err()
		rdr.Release()
		if err != nil {
			return rw.total, err
		}
	}
	if err := rw.end(); err != nil {
		return rw.total, err
	}
	// pqarrow's FileWriter.Close() closes the underlying file itself
	if s.closer != nil && s.format != "parquet" {
		if err := s.closer.Close(); err != nil {
			return rw.total, err
		}
	}
	if rw.maxRows > 0 && rw.total > rw.written {
		fmt.Fprintf(os.Stderr, "… %d rows total (showing %d; use -o or a LIMIT to change)\n", rw.total, rw.written)
	}
	return rw.total, nil
}

// autoMaxRows: the reading formats default to sane caps — table for terminal
// height, md so an agent's careless SELECT * can't flood its own context.
// Data formats (csv/json/jsonl/arrow/parquet) emit everything unless
// --max-rows is given explicitly; the true total always reports on stderr.
func autoMaxRows(flagVal int, format string, tableDefault int) int {
	if flagVal >= 0 {
		return flagVal
	}
	switch format {
	case "table":
		return tableDefault
	case "md":
		return 1000
	}
	return 0
}

// ── commands ────────────────────────────────────────────────────────────

// newFlagSet builds a FlagSet whose -h/--help shows the command's own usage
// line and example, not just bare flag defaults.
func newFlagSet(name, usage string) *flag.FlagSet {
	fs := flag.NewFlagSet(name, flag.ExitOnError)
	fs.Usage = func() {
		fmt.Fprintln(os.Stderr, usage)
		fmt.Fprintln(os.Stderr, "flags:")
		fs.PrintDefaults()
	}
	return fs
}

// parseFlags handles flags and positionals in any order (stdlib flag stops
// at the first positional, which surprises everyone eventually).
func parseFlags(fs *flag.FlagSet, args []string) []string {
	var pos []string
	for {
		fs.Parse(args)
		rest := fs.Args()
		if len(rest) == 0 {
			break
		}
		pos = append(pos, rest[0])
		args = rest[1:]
	}
	return pos
}

func cmdConnect(args []string) error {
	fs := newFlagSet("connect", `usage: sparrow connect <grpc[+tls]://host:port> [flags]
verify a Flight SQL server and save it as a profile (the first becomes default)
example: sparrow connect grpc+tls://flight.sparrowflight.io:443 --basic demo:demo`)
	basic := fs.String("basic", "", "user:pass (API key as user is fine)")
	bearer := fs.String("bearer", "", "bearer token (InfluxDB 3 style)")
	var hdrs multiFlag
	fs.Var(&hdrs, "header", "extra per-call metadata key=value (repeatable; e.g. --header database=mydb)")
	tlsSkip := fs.Bool("tls-skip-verify", false, "accept self-signed TLS certs")
	cert := fs.String("tls-cert", "", "client certificate for mTLS (PEM path)")
	key := fs.String("tls-key", "", "client private key for mTLS (PEM path)")
	ca := fs.String("tls-ca", "", "CA bundle to verify the server (PEM path)")
	name := fs.String("name", "default", "profile name")
	pos := parseFlags(fs, args)
	if len(pos) < 1 {
		return fmt.Errorf("usage: sparrow connect <grpc[+tls]://host:port> [--basic user:pass | --bearer TOKEN] [--header k=v] [--tls-cert/--tls-key/--tls-ca …] [--name profile]")
	}
	p := Profile{URI: pos[0], Auth: "none", TLSSkipVerify: *tlsSkip,
		TLSCert: *cert, TLSKey: *key, TLSCA: *ca}
	if *basic != "" {
		u, pw, _ := strings.Cut(*basic, ":")
		p.Auth, p.User, p.Pass = "basic", u, pw
	}
	if *bearer != "" {
		p.Auth, p.Token = "bearer", *bearer
	}
	h, err := parseHeaders(hdrs)
	if err != nil {
		return err
	}
	p.Headers = h

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	t0 := time.Now()
	cl, ctx, err := dial(ctx, p)
	if err != nil {
		return err
	}
	defer cl.Close()

	// fingerprint: GetSqlInfo → SELECT version() (probeVendor); last resort
	// SELECT 1 just proves the query path (never alias a FROM-less SELECT —
	// Dremio; see dialect-compat.md)
	serverDesc := probeVendor(ctx, cl)
	if serverDesc == "" {
		info, err := cl.Execute(ctx, "SELECT 1")
		if err != nil {
			return fmt.Errorf("connected but probes failed: %w", err)
		}
		rdr, err := cl.DoGet(ctx, info.Endpoint[0].Ticket)
		if err != nil {
			return fmt.Errorf("connected but probes failed: %w", err)
		}
		for rdr.Next() {
		}
		rdr.Release()
		serverDesc = "Flight SQL server (vendor info unsupported)"
	}

	cfg := loadConfig()
	cfg.Profiles[*name] = p
	if cfg.Default == "" || *name == "default" {
		cfg.Default = *name
	}
	if err := saveConfig(cfg); err != nil {
		return err
	}
	fmt.Printf("✓ connected in %d ms — %s\n", time.Since(t0).Milliseconds(), strings.TrimSpace(serverDesc))
	fmt.Printf("✓ saved profile %q (default: %s)\n", *name, cfg.Default)
	return nil
}

func cmdLs(args []string) error {
	fs := newFlagSet("ls", `usage: sparrow ls [pattern] [flags]
list tables via the GetTables RPC (works identically on every Flight SQL server)
example: sparrow ls -o md`)
	cf := addConnFlags(fs)
	output := fs.String("o", "", "output: table|csv|json|jsonl|md|arrow, or a file path (.parquet .csv …)")
	pos := parseFlags(fs, args)

	p, _, err := cf.resolve()
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	cl, ctx, err := dial(ctx, p)
	if err != nil {
		return err
	}
	defer cl.Close()

	var pattern *string
	if len(pos) > 0 {
		s := pos[0]
		pattern = &s
	}
	// GetTables RPC: the discovery path that works identically on every tested server.
	info, err := cl.GetTables(ctx, &flightsql.GetTablesOpts{TableNameFilterPattern: pattern})
	if err != nil {
		return err
	}
	s, err := resolveSink(*output)
	if err != nil {
		return err
	}
	_, err = consumeInfo(ctx, cl, info, s, autoMaxRows(-1, s.format, 1000))
	return err
}

func cmdSQL(args []string) error {
	fs := newFlagSet("sql", `usage: sparrow sql "SELECT ..." [flags]
run a Flight SQL statement; -o picks the output format or file
examples: sparrow sql "SELECT 42 AS x" -o md
          sparrow sql "SELECT * FROM t" -o data.parquet
          sparrow sql "SELECT * FROM t" | duckdb   (pipe = raw Arrow IPC)`)
	cf := addConnFlags(fs)
	maxRows := fs.Int("max-rows", -1, "max rows to emit (default: 40 table, 1000 md, unlimited otherwise)")
	output := fs.String("o", "", "output: table|csv|json|jsonl|md|arrow, or a file path (.parquet .csv …)")
	file := fs.String("f", "", "read the SQL from a file")
	encKey := fs.String("encrypt-key", "", "encrypt parquet output (Parquet Modular Encryption): hex, env:VAR or file:path")
	pos := parseFlags(fs, args)
	var query string
	switch {
	case *file != "":
		b, err := os.ReadFile(*file)
		if err != nil {
			return err
		}
		query = string(b)
	case len(pos) == 1 && pos[0] == "-":
		// SQL on stdin — no shell-quoting battles for long statements
		b, err := io.ReadAll(os.Stdin)
		if err != nil {
			return err
		}
		query = string(b)
	case len(pos) >= 1:
		query = strings.Join(pos, " ")
	default:
		return fmt.Errorf(`usage: sparrow sql "SELECT ..." | sparrow sql - | sparrow sql -f query.sql`)
	}

	p, _, err := cf.resolve()
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	cl, ctx, err := dial(ctx, p)
	if err != nil {
		return err
	}
	defer cl.Close()

	t0 := time.Now()
	info, err := cl.Execute(ctx, query)
	if err != nil {
		return err
	}
	s, err := resolveSink(*output)
	if err != nil {
		return err
	}
	if *encKey != "" {
		if s.format != "parquet" {
			return fmt.Errorf("--encrypt-key only applies to parquet output (-o data.parquet)")
		}
		k, err := loadKey(*encKey)
		if err != nil {
			return err
		}
		s.encKey = k
	}
	total, err := consumeInfo(ctx, cl, info, s, autoMaxRows(*maxRows, s.format, 40))
	if err != nil {
		return err
	}
	if s.path != "" {
		fmt.Fprintf(os.Stderr, "✓ %d rows → %s in %d ms\n", total, s.path, time.Since(t0).Milliseconds())
	} else if stdoutIsTTY() {
		fmt.Fprintf(os.Stderr, "✓ %d rows in %d ms\n", total, time.Since(t0).Milliseconds())
	}
	return nil
}

// probeVendor fingerprints the server: GetSqlInfo SERVER_NAME+VERSION first
// (EnergyScope, GizmoSQL, InfluxDB), then SELECT version() (Dremio errors on
// GetSqlInfo but answers version() — the exact reverse of InfluxDB).
func probeVendor(ctx context.Context, cl *flightsql.Client) string {
	if info, err := cl.GetSqlInfo(ctx, []flightsql.SqlInfo{
		flightsql.SqlInfoFlightSqlServerName, flightsql.SqlInfoFlightSqlServerVersion,
	}); err == nil {
		if rdr, err := cl.DoGet(ctx, info.Endpoint[0].Ticket); err == nil {
			vals := []string{}
			for rdr.Next() {
				rec := rdr.Record()
				if rec.NumCols() >= 2 {
					for r := 0; r < int(rec.NumRows()); r++ {
						switch cell(rec.Column(0), r) {
						case "0", "1":
							vals = append(vals, cell(rec.Column(1), r))
						}
					}
				}
			}
			rdr.Release()
			if v := strings.TrimSpace(strings.Join(vals, " ")); v != "" {
				return v
			}
		}
	}
	// no alias — FROM-less SELECTs can't take one on Dremio
	if info, err := cl.Execute(ctx, "SELECT version()"); err == nil {
		if rdr, err := cl.DoGet(ctx, info.Endpoint[0].Ticket); err == nil {
			v := ""
			for rdr.Next() {
				rec := rdr.Record()
				if v == "" && rec.NumRows() > 0 && rec.NumCols() > 0 {
					v = cell(rec.Column(0), 0)
				}
			}
			rdr.Release()
			return strings.TrimSpace(v)
		}
	}
	return ""
}

// cmdOrient prints a one-shot markdown orientation: vendor, every table,
// every schema. Designed so a single command tells an AI agent (or a human
// meeting a server for the first time) everything it needs to start querying.
func cmdOrient(args []string) error {
	fs := newFlagSet("orient", `usage: sparrow orient [flags]
one-shot markdown orientation: server vendor, every table, every schema
example: sparrow orient -s gizmo`)
	cf := addConnFlags(fs)
	parseFlags(fs, args)

	p, pname, err := cf.resolve()
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	cl, ctx, err := dial(ctx, p)
	if err != nil {
		return err
	}
	defer cl.Close()

	vendor := probeVendor(ctx, cl)
	if vendor == "" {
		vendor = "Flight SQL server (vendor info unsupported)"
	}
	fmt.Printf("# %s\n\n", vendor)
	fmt.Printf("endpoint: `%s` (profile: %s)\n\n", p.URI, pname)

	info, err := cl.GetTables(ctx, &flightsql.GetTablesOpts{IncludeSchema: true})
	if err != nil {
		return err
	}
	type tbl struct {
		catalog, schema, name, typ string
		arrow                      *arrow.Schema
	}
	var tables []tbl
	for _, ep := range info.Endpoint {
		rdr, err := cl.DoGet(ctx, ep.Ticket)
		if err != nil {
			return err
		}
		for rdr.Next() {
			rec := rdr.Record()
			idx := map[string]int{}
			for i, f := range rec.Schema().Fields() {
				idx[f.Name] = i
			}
			get := func(col string, r int) string {
				i, ok := idx[col]
				if !ok || rec.Column(i).IsNull(r) {
					return ""
				}
				return cell(rec.Column(i), r)
			}
			for r := 0; r < int(rec.NumRows()); r++ {
				t := tbl{
					catalog: get("catalog_name", r), schema: get("db_schema_name", r),
					name: get("table_name", r), typ: get("table_type", r),
				}
				if i, ok := idx["table_schema"]; ok && !rec.Column(i).IsNull(r) {
					var b []byte
					switch col := rec.Column(i).(type) {
					case *array.Binary:
						b = col.Value(r)
					case *array.LargeBinary:
						b = col.Value(r)
					}
					if b != nil {
						if sc, err := flight.DeserializeSchema(b, memory.DefaultAllocator); err == nil {
							t.arrow = sc
						}
					}
				}
				tables = append(tables, t)
			}
		}
		err = rdr.Err()
		rdr.Release()
		if err != nil {
			return err
		}
	}
	if len(tables) == 0 {
		fmt.Println("no tables visible (GetTables returned nothing)")
		return nil
	}

	fmt.Println("## tables")
	fmt.Println()
	fmt.Println("| catalog | schema | table | type |")
	fmt.Println("| --- | --- | --- | --- |")
	for _, t := range tables {
		fmt.Printf("| %s | %s | %s | %s |\n", t.catalog, t.schema, t.name, t.typ)
	}
	for _, t := range tables {
		if t.arrow == nil || t.arrow.NumFields() == 0 {
			continue // Dremio returns empty schemas in GetTables — skip the noise
		}
		fmt.Printf("\n## %s\n\n", t.name)
		fmt.Println("| column | type | nullable |")
		fmt.Println("| --- | --- | --- |")
		for _, f := range t.arrow.Fields() {
			null := ""
			if f.Nullable {
				null = "yes"
			}
			fmt.Printf("| %s | %s | %s |\n", f.Name, f.Type, null)
		}
	}
	fmt.Println("\nnext: `sparrow info <table>` for a row count · `sparrow sql \"SELECT ... LIMIT 20\" -o md` to look at data")
	return nil
}

func groupDigits(s string) string {
	if len(s) <= 3 {
		return s
	}
	var b strings.Builder
	lead := len(s) % 3
	if lead > 0 {
		b.WriteString(s[:lead])
	}
	for i := lead; i < len(s); i += 3 {
		if b.Len() > 0 {
			b.WriteByte(',')
		}
		b.WriteString(s[i : i+3])
	}
	return b.String()
}

func cmdInfo(args []string) error {
	fs := newFlagSet("info", `usage: sparrow info <table> [flags]
show a table's schema, catalog and row count before pulling anything
example: sparrow info series_data`)
	cf := addConnFlags(fs)
	noCount := fs.Bool("no-count", false, "skip the COUNT(*) row estimate")
	pos := parseFlags(fs, args)
	if len(pos) < 1 {
		return fmt.Errorf("usage: sparrow info <table> [-s profile] [--no-count]")
	}
	table := pos[0]

	p, _, err := cf.resolve()
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	cl, ctx, err := dial(ctx, p)
	if err != nil {
		return err
	}
	defer cl.Close()

	info, err := cl.GetTables(ctx, &flightsql.GetTablesOpts{
		TableNameFilterPattern: &table,
		IncludeSchema:          true,
	})
	if err != nil {
		return err
	}
	found := false
	var schema *arrow.Schema
	for _, ep := range info.Endpoint {
		rdr, err := cl.DoGet(ctx, ep.Ticket)
		if err != nil {
			return err
		}
		for rdr.Next() {
			rec := rdr.Record()
			idx := map[string]int{}
			for i, f := range rec.Schema().Fields() {
				idx[f.Name] = i
			}
			for r := 0; r < int(rec.NumRows()); r++ {
				found = true
				name := table
				if i, ok := idx["table_name"]; ok {
					name = cell(rec.Column(i), r)
				}
				fmt.Printf("table: %s", name)
				if i, ok := idx["table_type"]; ok {
					fmt.Printf(" (%s)", cell(rec.Column(i), r))
				}
				fmt.Println()
				cat, sch := "", ""
				if i, ok := idx["catalog_name"]; ok && !rec.Column(i).IsNull(r) {
					cat = cell(rec.Column(i), r)
				}
				if i, ok := idx["db_schema_name"]; ok && !rec.Column(i).IsNull(r) {
					sch = cell(rec.Column(i), r)
				}
				if cat != "" || sch != "" {
					fmt.Printf("catalog: %s · schema: %s\n", cat, sch)
				}
				if i, ok := idx["table_schema"]; ok && !rec.Column(i).IsNull(r) {
					var b []byte
					switch col := rec.Column(i).(type) {
					case *array.Binary:
						b = col.Value(r)
					case *array.LargeBinary:
						b = col.Value(r)
					}
					if b != nil {
						if sc, err := flight.DeserializeSchema(b, memory.DefaultAllocator); err == nil {
							schema = sc
						}
					}
				}
			}
		}
		err = rdr.Err()
		rdr.Release()
		if err != nil {
			return err
		}
	}
	if !found {
		return fmt.Errorf("no table matching %q (try: sparrow ls)", table)
	}
	if schema == nil {
		// server sent no embedded schema — probe with LIMIT 0
		if info, err := cl.Execute(ctx, "SELECT * FROM "+table+" LIMIT 0"); err == nil {
			if rdr, err := cl.DoGet(ctx, info.Endpoint[0].Ticket); err == nil {
				schema = rdr.Schema()
				for rdr.Next() {
				}
				rdr.Release()
			}
		}
	}
	if schema != nil {
		fmt.Println("columns:")
		w := tabwriter.NewWriter(os.Stdout, 2, 4, 2, ' ', 0)
		for _, f := range schema.Fields() {
			null := ""
			if f.Nullable {
				null = "nullable"
			}
			fmt.Fprintf(w, "  %s\t%s\t%s\n", f.Name, f.Type, null)
		}
		w.Flush()
	}
	if !*noCount {
		if info, err := cl.Execute(ctx, "SELECT COUNT(*) FROM "+table); err == nil {
			if rdr, err := cl.DoGet(ctx, info.Endpoint[0].Ticket); err == nil {
				for rdr.Next() {
					rec := rdr.Record()
					if rec.NumRows() > 0 && rec.NumCols() > 0 {
						fmt.Printf("rows: %s\n", groupDigits(cell(rec.Column(0), 0)))
					}
				}
				rdr.Release()
			}
		}
	}
	return nil
}

func cmdProfiles(args []string) error {
	cfg := loadConfig()
	if len(args) > 0 {
		switch args[0] {
		case "use":
			if len(args) < 2 {
				return fmt.Errorf("usage: sparrow profiles use <name>")
			}
			if _, ok := cfg.Profiles[args[1]]; !ok {
				return fmt.Errorf("unknown profile %q", args[1])
			}
			cfg.Default = args[1]
			if err := saveConfig(cfg); err != nil {
				return err
			}
			fmt.Printf("default profile: %s\n", args[1])
			return nil
		case "rm":
			if len(args) < 2 {
				return fmt.Errorf("usage: sparrow profiles rm <name>")
			}
			if _, ok := cfg.Profiles[args[1]]; !ok {
				return fmt.Errorf("unknown profile %q", args[1])
			}
			delete(cfg.Profiles, args[1])
			if cfg.Default == args[1] {
				cfg.Default = ""
				for name := range cfg.Profiles {
					cfg.Default = name
					break
				}
			}
			if err := saveConfig(cfg); err != nil {
				return err
			}
			fmt.Printf("removed profile %q (default: %s)\n", args[1], cfg.Default)
			return nil
		default:
			return fmt.Errorf("usage: sparrow profiles [use <name> | rm <name>]")
		}
	}
	if len(cfg.Profiles) == 0 {
		fmt.Println("no profiles — run: sparrow connect <uri>")
		return nil
	}
	w := tabwriter.NewWriter(os.Stdout, 2, 4, 2, ' ', 0)
	fmt.Fprintln(w, "NAME\tURI\tAUTH\tTLS")
	for name, p := range cfg.Profiles {
		mark := ""
		if name == cfg.Default {
			mark = " *"
		}
		tlsDesc := "-"
		if strings.Contains(p.URI, "tls") || strings.HasPrefix(p.URI, "grpcs") {
			tlsDesc = "on"
			if p.TLSSkipVerify {
				tlsDesc = "skip-verify"
			}
		}
		fmt.Fprintf(w, "%s%s\t%s\t%s\t%s\n", name, mark, p.URI, p.Auth, tlsDesc)
	}
	return w.Flush()
}

func usage() {
	fmt.Println(`sparrow ` + version + ` — Arrow Flight data. At the speed of the command line.

usage:
  sparrow connect <grpc[+tls]://host:port> [--basic user:pass] [--tls-skip-verify] [--name p]
  sparrow orient [-s profile]                     one-shot markdown map: vendor, tables, schemas
  sparrow ls [pattern] [-s profile] [-o format]
  sparrow info <table> [-s profile] [--no-count]
  sparrow sql "SELECT ..." [-s profile] [-o format|file] [--max-rows N]
  sparrow sql -                                   read SQL from stdin (also: -f query.sql)
  sparrow profiles [use <name> | rm <name>]
  sparrow version

output (-o): table · csv · json · jsonl · md · arrow — or a file path:
  data.parquet · data.csv · data.json · data.jsonl · data.arrow · data.md
Defaults: a TTY gets a table; a pipe streams raw Arrow IPC (composable with
DuckDB, Python, anything). Agents and scripts: use -o md, -o jsonl or -o csv.
Security: --tls-ca / --tls-cert / --tls-key for private CAs and mTLS;
  sql --encrypt-key <hex|env:VAR|file:path> seals parquet output (in-spec
  Parquet Modular Encryption — DuckDB/Spark/pyarrow read it back with the key).
Exit codes: 0 ok · 1 query error · 2 connection/auth · 3 usage.`)
}

func main() {
	if len(os.Args) < 2 {
		usage()
		os.Exit(3)
	}
	var err error
	switch os.Args[1] {
	case "connect":
		err = cmdConnect(os.Args[2:])
	case "ls":
		err = cmdLs(os.Args[2:])
	case "info":
		err = cmdInfo(os.Args[2:])
	case "orient":
		err = cmdOrient(os.Args[2:])
	case "sql":
		err = cmdSQL(os.Args[2:])
	case "profiles":
		err = cmdProfiles(os.Args[2:])
	case "version":
		fmt.Println("sparrow", version)
	case "help", "-h", "--help":
		// "sparrow help <command>" → that command's own -h
		if len(os.Args) > 2 {
			switch os.Args[2] {
			case "connect":
				err = cmdConnect([]string{"-h"})
			case "ls":
				err = cmdLs([]string{"-h"})
			case "info":
				err = cmdInfo([]string{"-h"})
			case "orient":
				err = cmdOrient([]string{"-h"})
			case "sql":
				err = cmdSQL([]string{"-h"})
			default:
				usage()
			}
			break
		}
		usage()
	default:
		usage()
		os.Exit(3)
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		// connection/auth/profile failures exit 2, query errors exit 1 —
		// gRPC dials lazily, so Unavailable/Unauthenticated surface at the
		// first RPC and are classified here rather than at dial time
		var ce connError
		if errors.As(err, &ce) {
			os.Exit(2)
		}
		if s, ok := status.FromError(err); ok {
			switch s.Code() {
			case codes.Unavailable, codes.Unauthenticated:
				os.Exit(2)
			}
		}
		os.Exit(1)
	}
}
