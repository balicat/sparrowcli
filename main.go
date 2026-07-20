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
	"math"
	"net"
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	"runtime/debug"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
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
	"google.golang.org/grpc/stats"
	"google.golang.org/grpc/status"
)

// version is stamped by goreleaser (-X main.version={{.Version}}) on releases
var version = "0.17.0-dev"

// versionString falls back to the Go module version for `go install` builds,
// which don't get the ldflags stamp.
func versionString() string {
	if !strings.HasSuffix(version, "-dev") {
		return version
	}
	if bi, ok := debug.ReadBuildInfo(); ok && bi.Main.Version != "" && bi.Main.Version != "(devel)" {
		return strings.TrimPrefix(bi.Main.Version, "v")
	}
	return version
}

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

// usageError marks bad invocations so main can exit 3 — a typo'd flag must
// not read as "server down" (2) or "bad SQL" (1) to a script.
type usageError struct{ err error }

func (e usageError) Error() string { return e.err.Error() }
func (e usageError) Unwrap() error { return e.err }

func usagef(format string, a ...any) error { return usageError{fmt.Errorf(format, a...)} }

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

// tlsConfigFor builds the profile's TLS client config (skip-verify, mTLS
// keypair, custom CA) — shared by dial() and doctor's handshake stage.
func tlsConfigFor(p Profile) (*tls.Config, error) {
	tc := &tls.Config{
		InsecureSkipVerify: p.TLSSkipVerify, // GizmoSQL ships self-signed by default
	}
	if p.TLSCert != "" || p.TLSKey != "" {
		pair, err := tls.LoadX509KeyPair(p.TLSCert, p.TLSKey)
		if err != nil {
			return nil, fmt.Errorf("mTLS keypair: %w", err)
		}
		tc.Certificates = []tls.Certificate{pair}
	}
	if p.TLSCA != "" {
		pem, err := os.ReadFile(p.TLSCA)
		if err != nil {
			return nil, fmt.Errorf("tls-ca: %w", err)
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(pem) {
			return nil, fmt.Errorf("tls-ca: no PEM certificates in %s", p.TLSCA)
		}
		tc.RootCAs = pool
	}
	return tc, nil
}

func dial(ctx context.Context, p Profile, extra ...grpc.DialOption) (*flightsql.Client, context.Context, error) {
	target, useTLS, err := parseURI(p.URI)
	if err != nil {
		return nil, ctx, connError{err}
	}
	var creds grpc.DialOption
	if useTLS {
		tc, err := tlsConfigFor(p)
		if err != nil {
			return nil, ctx, connError{err}
		}
		creds = grpc.WithTransportCredentials(credentials.NewTLS(tc))
	} else {
		creds = grpc.WithTransportCredentials(insecure.NewCredentials())
	}
	cl, err := flightsql.NewClient(target, nil, nil, append([]grpc.DialOption{creds}, extra...)...)
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
			label := "connection failed during the auth handshake"
			if st, ok := status.FromError(err); ok && st.Code() == codes.Unauthenticated {
				label = "auth failed"
			}
			return nil, ctx, connError{fmt.Errorf("%s: %w", label, err)}
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
	format    string
	w         io.Writer
	closer    io.Closer // file to close, nil for stdout
	path      string    // file path, "" for stdout
	encKey    []byte    // parquet only: Parquet Modular Encryption footer key
	bigintStr bool      // json/jsonl: emit int64/uint64 as quoted strings
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
		return sink{}, usagef("-o: unknown format or extension %q (formats: table csv json jsonl md arrow · files: .arrow .parquet .csv .json .jsonl .md)", o)
	}
	f, err := os.Create(o)
	if err != nil {
		return sink{}, err
	}
	return sink{format: format, w: f, closer: f, path: o}, nil
}

// reorderJSON rewrites one RecordToJSON row so keys follow the schema's
// column order instead of Go's alphabetical map order — values are kept as
// raw bytes, so int64 precision survives untouched.
// sqlReservedWords — reserved words that commonly double as COLUMN names, so
// an unquoted reference is a parser error. Deliberately excludes pure clause
// keywords (select/from/where/order/…) which would false-fire on any unrelated
// syntax error. The ones people actually hit: the tester tripped on
// end/start/rows.
var sqlReservedWords = map[string]bool{
	"end": true, "start": true, "rows": true, "range": true, "default": true,
	"table": true, "column": true, "values": true, "primary": true,
	"references": true, "constraint": true, "window": true, "returning": true,
	"interval": true, "grouping": true, "partition": true, "qualify": true,
}

// reservedWordHint spots a parser error caused by an unquoted reserved word in
// the query and suggests quoting it. Returns "" when it doesn't apply.
func reservedWordHint(query string, err error) string {
	e := strings.ToLower(err.Error())
	if !strings.Contains(e, "syntax error") && !strings.Contains(e, "parser error") &&
		!strings.Contains(e, "parsererror") {
		return ""
	}
	seen := map[string]bool{}
	var hits []string
	for _, tok := range strings.FieldsFunc(query, func(r rune) bool {
		return !(r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || r == '_')
	}) {
		lw := strings.ToLower(tok)
		if sqlReservedWords[lw] && !seen[lw] {
			seen[lw] = true
			hits = append(hits, tok)
		}
	}
	if len(hits) == 0 {
		return ""
	}
	quoted := make([]string, len(hits))
	for i, h := range hits {
		quoted[i] = `"` + h + `"`
	}
	return "reserved word(s) " + strings.Join(hits, ", ") +
		" must be quoted as an identifier: " + strings.Join(quoted, ", ")
}

// quoteRawNumber wraps a bare JSON number in quotes (for --bigint-as-string,
// so JS consumers keep full int64 precision). null / already-string pass through.
func quoteRawNumber(v json.RawMessage) json.RawMessage {
	s := strings.TrimSpace(string(v))
	if s == "" || s == "null" || s[0] == '"' {
		return v
	}
	return json.RawMessage(`"` + s + `"`)
}

func reorderJSON(line string, schema *arrow.Schema, bigintStr bool) string {
	var m map[string]json.RawMessage
	if json.Unmarshal([]byte(line), &m) != nil {
		return line
	}
	var b strings.Builder
	b.WriteByte('{')
	first := true
	emitKV := func(k string, v json.RawMessage) {
		if !first {
			b.WriteByte(',')
		}
		first = false
		nb, _ := json.Marshal(k)
		b.Write(nb)
		b.WriteByte(':')
		b.Write(v)
	}
	for _, f := range schema.Fields() {
		if v, ok := m[f.Name]; ok {
			if bigintStr && (f.Type.ID() == arrow.INT64 || f.Type.ID() == arrow.UINT64) {
				v = quoteRawNumber(v)
			}
			emitKV(f.Name, v)
			delete(m, f.Name)
		}
	}
	for k, v := range m { // leftovers, defensively
		emitKV(k, v)
	}
	b.WriteByte('}')
	return b.String()
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
	// sparkline sampling (table on a TTY): numeric columns are sampled from
	// the FULL stream, so the preview covers what the row cap hides
	sparkCols []int       // schema indices of the sampled columns (≤3)
	samples   [][]float64 // parallel to sparkCols, capped at sparkCap each
	sparkDone bool
}

const sparkCap = 4096

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
		if stdoutIsTTY() {
			for i, f := range schema.Fields() {
				if len(rw.sparkCols) >= 3 {
					break
				}
				switch f.Type.ID() {
				case arrow.INT8, arrow.INT16, arrow.INT32, arrow.INT64,
					arrow.UINT8, arrow.UINT16, arrow.UINT32, arrow.UINT64,
					arrow.FLOAT32, arrow.FLOAT64:
					rw.sparkCols = append(rw.sparkCols, i)
				}
			}
			rw.samples = make([][]float64, len(rw.sparkCols))
		}
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
	if len(rw.sparkCols) > 0 && !rw.sparkDone {
		rw.sample(rec) // from the full record, before any maxRows slicing
	}
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
			ln = reorderJSON(ln, rw.schema, rw.s.bigintStr)
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
		if err := rw.tw.Flush(); err != nil {
			return err
		}
		rw.sparklines()
		return nil
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

// sample collects numeric values for the sparkline, in arrival order.
func (rw *recordWriter) sample(rec arrow.Record) {
	done := true
	for si, ci := range rw.sparkCols {
		if ci >= int(rec.NumCols()) || len(rw.samples[si]) >= sparkCap {
			continue
		}
		col := rec.Column(ci)
		for r := 0; r < int(rec.NumRows()) && len(rw.samples[si]) < sparkCap; r++ {
			if col.IsNull(r) {
				continue
			}
			if f, err := strconv.ParseFloat(cell(col, r), 64); err == nil &&
				!math.IsNaN(f) && !math.IsInf(f, 0) {
				rw.samples[si] = append(rw.samples[si], f)
			}
		}
		if len(rw.samples[si]) < sparkCap {
			done = false
		}
	}
	rw.sparkDone = done
}

// sparklines draws the shape of each sampled column under the table — the
// row cap shows 40 rows, the sparkline shows the whole stream.
func (rw *recordWriter) sparklines() {
	tw := tabwriter.NewWriter(rw.s.w, 2, 4, 2, ' ', 0)
	any := false
	for si, ci := range rw.sparkCols {
		vals := rw.samples[si]
		if len(vals) < 8 {
			continue
		}
		lo, hi := vals[0], vals[0]
		for _, v := range vals[1:] {
			if v < lo {
				lo = v
			}
			if v > hi {
				hi = v
			}
		}
		fmt.Fprintf(tw, "%s\t%s\tmin %s · max %s\n", rw.schema.Field(ci).Name,
			spark(vals, 50), strconv.FormatFloat(lo, 'g', 6, 64), strconv.FormatFloat(hi, 'g', 6, 64))
		any = true
	}
	if any {
		tw.Flush()
	}
}

// spark renders vals as a width-char block line; each char is the mean of
// its bucket, scaled to the means' own min..max for contrast.
func spark(vals []float64, width int) string {
	if len(vals) < width {
		width = len(vals)
	}
	means := make([]float64, width)
	for b := 0; b < width; b++ {
		lo, hi := b*len(vals)/width, (b+1)*len(vals)/width
		if hi <= lo {
			hi = lo + 1
		}
		var s float64
		for _, v := range vals[lo:hi] {
			s += v
		}
		means[b] = s / float64(hi-lo)
	}
	mn, mx := means[0], means[0]
	for _, m := range means[1:] {
		if m < mn {
			mn = m
		}
		if m > mx {
			mx = m
		}
	}
	blocks := []rune("▁▂▃▄▅▆▇█")
	out := make([]rune, width)
	for i, m := range means {
		idx := 3 // flat series: mid-height
		if mx > mn {
			idx = int((m - mn) / (mx - mn) * 7.999)
			if idx > 7 {
				idx = 7
			}
		}
		out[i] = blocks[idx]
	}
	return string(out)
}

// qstats collects the anatomy of a query's data phase for sql --stats:
// timings, per-batch arrivals (pacing), and the per-column type/encoding/
// size breakdown of what the stream actually carried.
type qstats struct {
	gotFirst          bool
	firstMs, streamMs int64
	batches           int64
	rowsPerBatch      []float64
	waits             []float64 // ms idle between a batch's write end and the next arrival
	waitTotal         float64
	lastDone          time.Time
	colNames          []string
	colTypes          []string
	colBytes          []int64
	colNulls          []int64
	decoded           int64        // in-memory bytes after IPC decode (vs wire bytes)
	wireFn            func() int64 // snapshots DoGet wire bytes (set when a counter is attached)
	rampMs            []int64      // per-batch arrival time since stream start
	rampWire          []int64      // wire bytes received by that arrival
}

// arrayDataSize sums an array's buffer bytes, children and dictionary
// included — the decoded in-memory footprint of what came off the wire.
func arrayDataSize(d arrow.ArrayData) int64 {
	if d == nil {
		return 0
	}
	// Dictionary()/Children() hand back typed-nil *array.Data for plain
	// columns — a non-nil interface wrapping nil that panics on first use
	if cd, ok := d.(*array.Data); ok && cd == nil {
		return 0
	}
	var n int64
	for _, b := range d.Buffers() {
		if b != nil {
			n += int64(b.Len())
		}
	}
	for _, c := range d.Children() {
		n += arrayDataSize(c)
	}
	if dd, ok := d.(interface{ Dictionary() arrow.ArrayData }); ok {
		if dict := dd.Dictionary(); dict != nil {
			n += arrayDataSize(dict)
		}
	}
	return n
}

// encodingOf names the arrow-level encoding a column arrives with.
func encodingOf(typeStr string) string {
	switch {
	case strings.HasPrefix(typeStr, "dictionary"):
		return "dict"
	case strings.HasPrefix(typeStr, "run_end_encoded"):
		return "ree"
	}
	return "plain"
}

func fmtBytes(n int64) string {
	if n >= 1e6 {
		return fmt.Sprintf("%.1f MB", float64(n)/1e6)
	}
	if n >= 1e3 {
		return fmt.Sprintf("%.1f KB", float64(n)/1e3)
	}
	return fmt.Sprintf("%d B", n)
}

func (st *qstats) observe(rec arrow.Record, tStream time.Time) {
	now := time.Now()
	if !st.gotFirst {
		st.gotFirst = true
		st.firstMs = now.Sub(tStream).Milliseconds()
	} else if !st.lastDone.IsZero() {
		w := float64(now.Sub(st.lastDone).Microseconds()) / 1000
		st.waits = append(st.waits, w)
		st.waitTotal += w
	}
	st.batches++
	st.rowsPerBatch = append(st.rowsPerBatch, float64(rec.NumRows()))
	if st.colNames == nil {
		for _, f := range rec.Schema().Fields() {
			st.colNames = append(st.colNames, f.Name)
			st.colTypes = append(st.colTypes, f.Type.String())
		}
		st.colBytes = make([]int64, len(st.colNames))
		st.colNulls = make([]int64, len(st.colNames))
	}
	for i, col := range rec.Columns() {
		if i < len(st.colBytes) {
			b := arrayDataSize(col.Data())
			st.colBytes[i] += b
			st.decoded += b
			st.colNulls[i] += int64(col.NullN())
		}
	}
	if st.wireFn != nil {
		st.rampMs = append(st.rampMs, now.Sub(tStream).Milliseconds())
		st.rampWire = append(st.rampWire, st.wireFn())
	}
}

// byteCounter is a grpc stats.Handler that counts payload bytes as they cross
// the wire — the honest transfer number for sql --stats throughput. It also
// keeps the first few FlightData headers so --stats can report the DECLARED
// IPC body-compression codec, not just infer one from the wire/decode ratio.
type byteCounter struct {
	in, out     atomic.Int64
	msgs        atomic.Int64 // FlightData messages seen
	recBatches  atomic.Int64 // record-batch messages on the stream
	dictBatches atomic.Int64 // dictionary-batch messages on the stream
	capN        int          // how many headers to keep (0 → 8)
	mu          sync.Mutex
	headers     [][]byte
}

func (b *byteCounter) TagRPC(ctx context.Context, _ *stats.RPCTagInfo) context.Context { return ctx }
func (b *byteCounter) HandleRPC(_ context.Context, s stats.RPCStats) {
	switch v := s.(type) {
	case *stats.InPayload:
		b.in.Add(int64(v.WireLength))
		if fd, ok := v.Payload.(*flight.FlightData); ok && len(fd.DataHeader) > 0 {
			b.msgs.Add(1)
			if m, ok := ipcHeaderInfo(fd.DataHeader); ok {
				switch {
				case m.Typ == "record batch":
					b.recBatches.Add(1)
				case strings.HasPrefix(m.Typ, "dictionary"):
					b.dictBatches.Add(1)
				}
			}
			b.mu.Lock()
			max := b.capN
			if max <= 0 {
				max = 8
			}
			if len(b.headers) < max {
				b.headers = append(b.headers, append([]byte(nil), fd.DataHeader...))
			}
			b.mu.Unlock()
		}
	case *stats.OutPayload:
		b.out.Add(int64(v.WireLength))
	}
}

func (b *byteCounter) snapshotHeaders() [][]byte {
	b.mu.Lock()
	defer b.mu.Unlock()
	return append([][]byte(nil), b.headers...)
}

// declaredCodec walks the captured headers for the first RecordBatch message
// and returns its declared body-compression codec ("lz4_frame", "zstd"), or
// "" when no BodyCompression table is present (an uncompressed stream).
func (b *byteCounter) declaredCodec() (codec string, sawBatch bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	for _, h := range b.headers {
		if c, isBatch := ipcCodec(h); isBatch {
			return c, true
		}
	}
	return "", false
}

// ── minimal flatbuffer walk over the Arrow IPC Message header ───────────
//
// arrow-go's generated flatbuffer code is internal, and the ipc reader
// decompresses transparently without exposing the codec — so we read the
// three fields we need straight from the (frozen, spec'd) binary format:
// Message.header_type (field 1), Message.header (field 2) → RecordBatch.
// compression (field 3) → BodyCompression.codec (field 0; the enum default
// 0 = LZ4_FRAME is elided by flatbuffers, so table-present + field-absent
// still means lz4).

func fbU16(b []byte, off int) (uint16, bool) {
	if off < 0 || off+2 > len(b) {
		return 0, false
	}
	return uint16(b[off]) | uint16(b[off+1])<<8, true
}

func fbU32(b []byte, off int) (uint32, bool) {
	if off < 0 || off+4 > len(b) {
		return 0, false
	}
	return uint32(b[off]) | uint32(b[off+1])<<8 | uint32(b[off+2])<<16 | uint32(b[off+3])<<24, true
}

// fbField resolves table field i to its data offset (0 = field absent).
func fbField(b []byte, table int, i int) int {
	soff, ok := fbU32(b, table) // int32 soffset to vtable
	if !ok {
		return 0
	}
	vt := table - int(int32(soff))
	vsize, ok := fbU16(b, vt)
	if !ok || 4+2*i+2 > int(vsize) {
		return 0
	}
	fo, ok := fbU16(b, vt+4+2*i)
	if !ok || fo == 0 {
		return 0
	}
	return table + int(fo)
}

// fbTable follows an offset field to the sub-table it points at.
func fbTable(b []byte, fieldOff int) int {
	uoff, ok := fbU32(b, fieldOff)
	if !ok {
		return 0
	}
	return fieldOff + int(uoff)
}

func fbI64(b []byte, off int) (int64, bool) {
	if off < 0 || off+8 > len(b) {
		return 0, false
	}
	var v uint64
	for i := 7; i >= 0; i-- {
		v = v<<8 | uint64(b[off+i])
	}
	return int64(v), true
}

// ipcMsg is one Arrow IPC message's manifest, read from its header alone.
type ipcMsg struct {
	Typ   string // schema | dictionary | record batch
	Rows  int64
	Body  int64  // body bytes that followed this header on the wire
	Codec string // "" = no BodyCompression table
	Meta  int    // custom_metadata key-value count
}

func fbBatchInto(h []byte, rb int, m *ipcMsg) {
	if lOff := fbField(h, rb, 0); lOff != 0 { // RecordBatch.length
		m.Rows, _ = fbI64(h, lOff)
	}
	if cOff := fbField(h, rb, 3); cOff != 0 { // RecordBatch.compression
		comp := fbTable(h, cOff)
		m.Codec = "lz4_frame" // flatbuffers elide the default enum value
		if codOff := fbField(h, comp, 0); codOff != 0 && codOff < len(h) {
			switch h[codOff] {
			case 0:
				m.Codec = "lz4_frame"
			case 1:
				m.Codec = "zstd"
			default:
				m.Codec = fmt.Sprintf("codec(%d)", h[codOff])
			}
		}
	}
}

func ipcHeaderInfo(h []byte) (ipcMsg, bool) {
	var m ipcMsg
	rootOff, ok := fbU32(h, 0)
	if !ok {
		return m, false
	}
	msg := int(rootOff)
	if blOff := fbField(h, msg, 3); blOff != 0 { // Message.bodyLength
		m.Body, _ = fbI64(h, blOff)
	}
	if kvOff := fbField(h, msg, 4); kvOff != 0 { // Message.custom_metadata
		if n, ok := fbU32(h, fbTable(h, kvOff)); ok {
			m.Meta = int(n)
		}
	}
	htOff := fbField(h, msg, 1) // MessageHeader union type
	hOff := fbField(h, msg, 2)  // union value
	if htOff == 0 || htOff >= len(h) || hOff == 0 {
		return m, false
	}
	tbl := fbTable(h, hOff)
	switch h[htOff] {
	case 1:
		m.Typ = "schema"
	case 2:
		m.Typ = "dictionary"
		if idOff := fbField(h, tbl, 0); idOff != 0 { // DictionaryBatch.id
			if id, ok := fbI64(h, idOff); ok {
				m.Typ = fmt.Sprintf("dictionary id=%d", id)
			}
		}
		if dOff := fbField(h, tbl, 1); dOff != 0 { // DictionaryBatch.data
			fbBatchInto(h, fbTable(h, dOff), &m)
		}
	case 3:
		m.Typ = "record batch"
		fbBatchInto(h, tbl, &m)
	default:
		m.Typ = fmt.Sprintf("type(%d)", h[htOff])
	}
	return m, true
}

func ipcCodec(header []byte) (codec string, isRecordBatch bool) {
	m, ok := ipcHeaderInfo(header)
	if !ok || m.Typ != "record batch" {
		return "", false
	}
	return m.Codec, true
}
func (b *byteCounter) TagConn(ctx context.Context, _ *stats.ConnTagInfo) context.Context { return ctx }
func (b *byteCounter) HandleConn(context.Context, stats.ConnStats)                       {}

func consumeInfo(ctx context.Context, cl *flightsql.Client, info *flight.FlightInfo, s sink, maxRows int, st *qstats) (int64, error) {
	rw := &recordWriter{s: s, maxRows: maxRows}
	tStream := time.Now()
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
			if st != nil {
				st.observe(rdr.Record(), tStream)
			}
			if err := rw.write(rdr.Record()); err != nil {
				rdr.Release()
				return rw.total, err
			}
			if st != nil {
				st.lastDone = time.Now() // waits exclude our own sink-write time
			}
		}
		err = rdr.Err()
		rdr.Release()
		if err != nil {
			return rw.total, err
		}
	}
	if st != nil {
		st.streamMs = time.Since(tStream).Milliseconds()
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
		fmt.Fprintf(os.Stderr, "… %d rows total (showing %d — raise --max-rows or add a LIMIT)\n", rw.total, rw.written)
	}
	return rw.total, nil
}

// autoMaxRows: the reading formats default to sane caps — table for terminal
// height, md so an agent's careless SELECT * can't flood its own context.
// Data formats (csv/json/jsonl/arrow/parquet) emit everything unless
// --max-rows is given explicitly; the true total always reports on stderr.
func autoMaxRows(flagVal int, format string, tableDefault int, toFile bool) int {
	if flagVal >= 0 {
		return flagVal
	}
	if toFile { // an explicit file sink gets everything
		return 0
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
// line and example, not just bare flag defaults. ContinueOnError so parse
// failures exit 3 (usage), not the flag package's exit 2 — which would read
// as "connection failed" to anything scripting our exit codes.
func newFlagSet(name, usage string) *flag.FlagSet {
	fs := flag.NewFlagSet(name, flag.ContinueOnError)
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
		if err := fs.Parse(args); err != nil {
			if errors.Is(err, flag.ErrHelp) {
				os.Exit(0)
			}
			os.Exit(3) // the flag package already printed error + usage
		}
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
		return usagef("usage: sparrow connect <grpc[+tls]://host:port> [--basic user:pass | --bearer TOKEN] [--header k=v] [--tls-cert/--tls-key/--tls-ca …] [--name profile]")
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
list tables via GetTables; the pattern is a SQL LIKE pattern interpreted by
the SERVER (% = any run, _ = one char, case-sensitive), e.g. "series_%"
example: sparrow ls "series_%" -o md`)
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
	_, err = consumeInfo(ctx, cl, info, s, autoMaxRows(-1, s.format, 1000, s.path != ""), nil)
	return err
}

func cmdSQL(args []string) error {
	fs := newFlagSet("sql", `usage: sparrow sql "SELECT ..." [flags]
run a Flight SQL statement; -o picks the output format or file
examples: sparrow sql "SELECT 42 AS x" -o md
          sparrow sql "SELECT * FROM t" -o data.parquet
          sparrow sql "SELECT * FROM t" | duckdb   (pipe = raw Arrow IPC)
          sparrow sql --substrait plan.pb          (execute a Substrait plan)`)
	cf := addConnFlags(fs)
	maxRows := fs.Int("max-rows", -1, "max rows to emit (default: 40 table, 1000 md, unlimited otherwise)")
	output := fs.String("o", "", "output: table|csv|json|jsonl|md|arrow, or a file path (.parquet .csv …)")
	file := fs.String("f", "", "read the SQL from a file")
	substrait := fs.String("substrait", "", "execute a serialized Substrait plan from this file instead of SQL (sparrow stays a client, not a planner)")
	encKey := fs.String("encrypt-key", "", "encrypt parquet output (Parquet Modular Encryption): hex, env:VAR or file:path")
	statsOn := fs.Bool("stats", false, "print the query's anatomy to stderr: plan / first byte / stream, rows, wire bytes, throughput")
	ipcOn := fs.Bool("ipc", false, "reveal the stream's IPC message manifest on stderr: message type, rows, body bytes, codec, custom metadata")
	schemaOnly := fs.Bool("schema", false, "print the result's column names + Arrow types and exit — no rows fetched")
	bigintStr := fs.Bool("bigint-as-string", false, "emit int64/uint64 as quoted strings in json/jsonl (preserve precision for JS consumers)")
	pos := parseFlags(fs, args)
	var query string
	var plan []byte
	switch {
	case *substrait != "":
		if len(pos) > 0 || *file != "" {
			return usagef("--substrait replaces the SQL text — drop the query argument / -f")
		}
		b, err := os.ReadFile(*substrait)
		if err != nil {
			return err
		}
		if len(b) == 0 {
			return usagef("%s is empty — expected a serialized Substrait plan", *substrait)
		}
		plan = b
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
		return usagef(`usage: sparrow sql "SELECT ..." | sparrow sql - | sparrow sql -f query.sql`)
	}

	if *schemaOnly {
		if plan != nil {
			return usagef("--schema and --substrait can't be combined")
		}
		return printQuerySchema(cf, query)
	}
	return execStatement(cf, query, plan, nil, *output, *encKey, *maxRows, *statsOn, *ipcOn, *bigintStr)
}

// printQuerySchema executes a query but reads only the result's schema —
// the server plans it and returns the Arrow schema without us draining rows.
func printQuerySchema(cf *connFlags, query string) error {
	p, _, err := cf.resolve()
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	cl, ctx, err := dial(ctx, p)
	if err != nil {
		return err
	}
	defer cl.Close()
	info, err := cl.Execute(ctx, query)
	if err != nil {
		return err
	}
	var schema *arrow.Schema
	if len(info.Schema) > 0 {
		schema, _ = flight.DeserializeSchema(info.Schema, memory.DefaultAllocator)
	}
	if schema == nil { // some servers only declare the schema on the stream
		rdr, err := cl.DoGet(ctx, info.Endpoint[0].Ticket)
		if err != nil {
			return err
		}
		schema = rdr.Schema()
		rdr.Release()
	}
	if schema == nil {
		return fmt.Errorf("no schema returned")
	}
	tw := tabwriter.NewWriter(os.Stdout, 2, 4, 2, ' ', 0)
	fmt.Fprintln(tw, "column\ttype\tnullable")
	for _, f := range schema.Fields() {
		null := ""
		if f.Nullable {
			null = "yes"
		}
		fmt.Fprintf(tw, "%s\t%s\t%s\n", f.Name, f.Type, null)
	}
	return tw.Flush()
}

// execStatement is the shared execution core behind sql and query:
// dial, run, sink, and the optional --stats / --ipc reports. A non-nil
// plan executes as a Substrait plan (CommandStatementSubstraitPlan)
// instead of SQL text — gated on the server's advertised capability.
func execStatement(cf *connFlags, query string, plan []byte, ticket []byte, output, encKey string, maxRows int, statsOn, ipcOn, bigintStr bool) error {
	p, _, err := cf.resolve()
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	var counter *byteCounter
	var extra []grpc.DialOption
	if statsOn || ipcOn {
		counter = &byteCounter{}
		if ipcOn {
			counter.capN = 128
		}
		extra = append(extra, grpc.WithStatsHandler(counter))
	}
	cl, ctx, err := dial(ctx, p, extra...)
	if err != nil {
		return err
	}
	defer cl.Close()

	var info *flight.FlightInfo
	var t0 time.Time
	if ticket != nil {
		// 1-RTT direct ticket: no GetFlightInfo, no SQL — the ticket goes
		// straight to DoGet via a synthetic single-endpoint FlightInfo.
		// Works on servers that accept client-constructed tickets (Sparrow:
		// JSON {"series": [...]}); opaque-handle servers reject it.
		t0 = time.Now()
		info = &flight.FlightInfo{Endpoint: []*flight.FlightEndpoint{
			{Ticket: &flight.Ticket{Ticket: ticket}}}}
	} else if plan != nil {
		// the tester's spec: pre-check SqlInfo code 5 and fail with a clear
		// message instead of firing the plan into a raw Unimplemented
		switch substraitAdvertised(ctx, cl) {
		case "false":
			return fmt.Errorf("server advertises Substrait=False (GetSqlInfo code 5) — it will not accept a plan; run: sparrow doctor --server")
		case "true":
		default:
			fmt.Fprintln(os.Stderr, "note: server does not advertise Substrait support (GetSqlInfo code 5 absent) — attempting anyway")
		}
		t0 = time.Now()
		info, err = cl.ExecuteSubstrait(ctx, flightsql.SubstraitPlan{Plan: plan})
	} else {
		t0 = time.Now()
		info, err = cl.Execute(ctx, query)
	}
	if err != nil {
		if h := reservedWordHint(query, err); h != "" {
			// pre-strip the gRPC "Detail:" tail: main()'s strip is anchored to
			// end-of-string and would miss it once the hint is appended
			msg := grpcDetailRe.ReplaceAllString(err.Error(), "")
			return fmt.Errorf("%s\nhint: %s", msg, h)
		}
		return err
	}
	planMs := time.Since(t0).Milliseconds()
	var wireAtPlan int64
	if counter != nil {
		wireAtPlan = counter.in.Load()
	}
	s, err := resolveSink(output)
	if err != nil {
		return err
	}
	if encKey != "" {
		if s.format != "parquet" {
			return fmt.Errorf("--encrypt-key only applies to parquet output (-o data.parquet)")
		}
		k, err := loadKey(encKey)
		if err != nil {
			return err
		}
		if len(k) != 32 {
			fmt.Fprintln(os.Stderr, "note: prefer 32-byte keys for DuckDB read-back — base64 of a"+
				" 16/24-byte key is 24/32 chars, which DuckDB misreads as a raw key")
		}
		s.encKey = k
	}
	s.bigintStr = bigintStr
	var st *qstats
	if statsOn {
		st = &qstats{}
		base := wireAtPlan
		st.wireFn = func() int64 { return counter.in.Load() - base }
	}
	total, err := consumeInfo(ctx, cl, info, s, autoMaxRows(maxRows, s.format, 40, s.path != ""), st)
	if err != nil {
		return err
	}
	if statsOn {
		wire := counter.in.Load() - wireAtPlan // DoGet payloads only
		var mbit float64
		if st.streamMs > 0 {
			mbit = float64(wire) * 8 / 1e6 / (float64(st.streamMs) / 1000)
		}
		planLabel := "plan (GetFlightInfo)"
		if ticket != nil {
			planLabel = "plan (skipped: 1-RTT)"
		}
		fmt.Fprintf(os.Stderr, `── query stats ─────────────────────────
%s %6d ms
first byte            %6d ms
stream (DoGet)        %6d ms
total                 %6d ms
`, fmt.Sprintf("%-21s", planLabel), planMs, st.firstMs, st.streamMs, time.Since(t0).Milliseconds())

		rowsLine := fmt.Sprintf("rows       %s in %d batches", groupDigits(fmt.Sprint(total)), st.batches)
		if len(st.rowsPerBatch) > 1 {
			rp := append([]float64(nil), st.rowsPerBatch...)
			sort.Float64s(rp)
			rowsLine += fmt.Sprintf(" · rows/batch p50 %s (min %s · max %s)",
				groupDigits(fmt.Sprintf("%.0f", percentile(rp, 0.5))),
				groupDigits(fmt.Sprintf("%.0f", rp[0])),
				groupDigits(fmt.Sprintf("%.0f", rp[len(rp)-1])))
		}
		fmt.Fprintln(os.Stderr, rowsLine)

		wireLine := fmt.Sprintf("wire       %s received", fmtBytes(wire))
		if st.decoded > 0 && wire >= 2048 { // ratios on tiny payloads are noise
			wireLine += fmt.Sprintf(" · decodes to %s (%.1f×)", fmtBytes(st.decoded),
				float64(st.decoded)/float64(wire))
		}
		if codec, saw := counter.declaredCodec(); saw {
			if codec == "" {
				wireLine += " · no body compression declared"
			} else {
				wireLine += " · codec " + codec
			}
		}
		fmt.Fprintln(os.Stderr, wireLine)
		if d := counter.dictBatches.Load(); d > 0 {
			noun := "batches"
			if d == 1 {
				noun = "batch"
			}
			fmt.Fprintf(os.Stderr, "dicts      %d dictionary %s on the stream\n", d, noun)
		}
		if wire >= 100_000 {
			fmt.Fprintf(os.Stderr, "speed      %.0f Mbit/s over the stream\n", mbit)
		}
		// ramp: how much of the stream landed in the first second vs overall —
		// on long streams this separates TCP/window warm-up from steady state
		if st.streamMs >= 2000 && len(st.rampMs) > 0 {
			var at1s int64
			for i, ms := range st.rampMs {
				if ms > 1000 {
					break
				}
				at1s = st.rampWire[i]
			}
			if at1s > 0 {
				fmt.Fprintf(os.Stderr, "ramp       first 1 s %.0f Mbit/s → overall %.0f Mbit/s\n",
					float64(at1s)*8/1e6, mbit)
			}
		}

		// pacing: waits exclude local sink-write time, so the gaps are the
		// sender + network, not us
		if len(st.waits) > 0 && st.streamMs > 0 {
			ws := append([]float64(nil), st.waits...)
			sort.Float64s(ws)
			pct := 100 * st.waitTotal / float64(st.streamMs)
			verdict := "mixed"
			switch {
			case pct >= 50:
				verdict = "paced upstream: sender or network stalls between batches"
			case pct < 20:
				verdict = "wire-paced: batches arrive back-to-back"
			}
			fmt.Fprintf(os.Stderr, "pacing     gaps p50 %.1f ms · p95 %.1f ms · max %.1f ms — %.0f%% of the stream is waiting (%s)\n",
				percentile(ws, 0.5), percentile(ws, 0.95), ws[len(ws)-1], pct, verdict)
		}

		// the stream's type/encoding anatomy — what the columns arrive AS
		if len(st.colNames) > 0 && st.decoded > 0 {
			tw := tabwriter.NewWriter(os.Stderr, 2, 4, 2, ' ', 0)
			fmt.Fprintln(tw, "column\ttype\tencoding\tnulls\tdecoded")
			for i, name := range st.colNames {
				fmt.Fprintf(tw, "%s\t%s\t%s\t%s\t%s (%.0f%%)\n",
					name, st.colTypes[i], encodingOf(st.colTypes[i]),
					groupDigits(fmt.Sprint(st.colNulls[i])), fmtBytes(st.colBytes[i]),
					100*float64(st.colBytes[i])/float64(st.decoded))
			}
			tw.Flush()
		}
	}
	if ipcOn {
		hdrs := counter.snapshotHeaders()
		total := counter.msgs.Load()
		fmt.Fprintln(os.Stderr, "── ipc messages ────────────────────────")
		tw := tabwriter.NewWriter(os.Stderr, 2, 4, 2, ' ', 0)
		fmt.Fprintln(tw, "#\tmessage\trows\tbody\tcodec\tmeta")
		shown := 0
		for i, h := range hdrs {
			if shown >= 20 {
				break
			}
			m, ok := ipcHeaderInfo(h)
			if !ok {
				continue
			}
			rows, codec := "—", m.Codec
			if m.Typ != "schema" {
				rows = groupDigits(fmt.Sprint(m.Rows))
				if codec == "" {
					codec = "none"
				}
			} else {
				codec = "—"
			}
			fmt.Fprintf(tw, "%d\t%s\t%s\t%s\t%s\t%d\n", i+1, m.Typ, rows, fmtBytes(m.Body), codec, m.Meta)
			shown++
		}
		tw.Flush()
		if int(total) > shown {
			fmt.Fprintf(os.Stderr, "… %s messages total (first %d shown)\n", groupDigits(fmt.Sprint(total)), shown)
		}
	}
	if statsOn {
		// --stats prints its own rows line above
	} else if s.path != "" {
		fmt.Fprintf(os.Stderr, "✓ %d rows → %s in %d ms\n", total, s.path, time.Since(t0).Milliseconds())
	} else if stdoutIsTTY() {
		fmt.Fprintf(os.Stderr, "✓ %d rows in %d ms\n", total, time.Since(t0).Milliseconds())
	}
	if s.path != "" && statsOn {
		fmt.Fprintf(os.Stderr, "✓ → %s\n", s.path)
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
		// A MACRO is a function, not a table — a bare SELECT * FROM it
		// errors. Teach the call shape right where it's discovered (the
		// generic "SELECT ... LIMIT 20" footer would mislead here).
		if strings.EqualFold(t.typ, "MACRO") {
			fmt.Printf("\na table macro — CALL it with arguments: `SELECT * FROM %s('...')` "+
				"(a bare `SELECT * FROM %s` errors; argument names: "+
				"`SELECT parameters FROM duckdb_functions() WHERE function_name='%s'`)\n",
				t.name, t.name, t.name)
		}
	}
	fmt.Println("\nnext: `sparrow info <table>` for a row count · `sparrow sql \"SELECT ... LIMIT 20\" -o md` to look at data (macros: call with args instead)")
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
		return usagef("usage: sparrow info <table> [-s profile] [--no-count]")
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
	isMacro := false
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
					typ := cell(rec.Column(i), r)
					fmt.Printf(" (%s)", typ)
					if strings.EqualFold(typ, "MACRO") {
						isMacro = true
					}
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
	if isMacro {
		// A macro is a function, not a table: no row count to take, and a
		// bare SELECT * FROM it errors — print the call shape instead.
		fmt.Printf("a table macro — call it with arguments: SELECT * FROM %s('...')\n"+
			"argument names: SELECT parameters FROM duckdb_functions() WHERE function_name='%s'\n",
			table, table)
		return nil
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
				return usagef("usage: sparrow profiles use <name>")
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
				return usagef("usage: sparrow profiles rm <name>")
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
			return usagef("usage: sparrow profiles [use <name> | rm <name>]")
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

// ── doctor ──────────────────────────────────────────────────────────────
//
// Connection failures all look the same from a client ("connection error")
// but live at different layers: DNS, a closed port, a TLS interceptor, a
// proxy that won't speak h2, rejected credentials. doctor walks the stack
// one layer at a time and names the one that breaks.

type checkResult struct {
	Check  string   `json:"check"`
	Status string   `json:"status"` // ok | warn | fail | skip
	Detail string   `json:"detail,omitempty"`
	Lines  []string `json:"lines,omitempty"` // extra evidence (e.g. presented cert chain)
	Hint   string   `json:"hint,omitempty"`
	Ms     int64    `json:"ms,omitempty"`
}

type doctorReport struct {
	Endpoint string        `json:"endpoint"`
	Profile  string        `json:"profile"`
	Table    string        `json:"table,omitempty"`
	Checks   []checkResult `json:"checks"`
	OK       bool          `json:"ok"`
}

type doctor struct {
	rep                     doctorReport
	json                    bool
	oks, warns, fails, errs int
	firstFail               string
}

var statusMark = map[string]string{"ok": "✓", "warn": "⚠", "fail": "✗", "skip": "·", "error": "!"}

func (d *doctor) emit(r checkResult) {
	d.rep.Checks = append(d.rep.Checks, r)
	switch r.Status {
	case "ok":
		d.oks++
	case "warn":
		d.warns++
	case "fail":
		d.fails++
		if d.firstFail == "" {
			d.firstFail = r.Check
		}
	case "error":
		d.errs++
	}
	if d.json {
		return
	}
	line := fmt.Sprintf(" %s %-9s %s", statusMark[r.Status], r.Check, r.Detail)
	if r.Ms > 0 {
		line += fmt.Sprintf(" (%d ms)", r.Ms)
	}
	fmt.Println(line)
	for _, l := range r.Lines {
		fmt.Println("             " + l)
	}
	if r.Hint != "" {
		fmt.Println("             hint: " + r.Hint)
	}
}

func tlsVersionName(v uint16) string {
	switch v {
	case tls.VersionTLS13:
		return "TLS 1.3"
	case tls.VersionTLS12:
		return "TLS 1.2"
	case tls.VersionTLS11:
		return "TLS 1.1"
	case tls.VersionTLS10:
		return "TLS 1.0"
	}
	return fmt.Sprintf("TLS 0x%04x", v)
}

func certLine(c *x509.Certificate) string {
	subj := c.Subject.CommonName
	if subj == "" && len(c.DNSNames) > 0 {
		subj = c.DNSNames[0]
	}
	iss := c.Issuer.CommonName
	if len(c.Issuer.Organization) > 0 {
		iss += " (" + c.Issuer.Organization[0] + ")"
	}
	return fmt.Sprintf("subject %q · issuer %q · expires %s", subj, iss,
		c.NotAfter.Format("2006-01-02"))
}

// interceptorNames — TLS interception products (AV HTTPS scanning, corporate
// proxies) that re-sign traffic with a locally-trusted root. A verified chain
// whose issuer matches one means the server's real certificate never reached
// this machine.
var interceptorNames = []string{
	"Norton", "Avast", "AVG ", "Kaspersky", "ESET", "Bitdefender", "McAfee",
	"Sophos", "Zscaler", "Fortinet", "FortiGate", "Blue Coat", "Forcepoint",
	"Palo Alto", "WatchGuard", "Web/Mail Shield",
}

func interceptorIn(chain []*x509.Certificate) string {
	for _, c := range chain {
		iss := c.Issuer.CommonName + " " + strings.Join(c.Issuer.Organization, " ")
		for _, n := range interceptorNames {
			if strings.Contains(iss, n) {
				return strings.TrimSpace(n)
			}
		}
	}
	return ""
}

// grpcCode unwraps connError and wrapped errors down to the gRPC status code.
func grpcCode(err error) codes.Code {
	var ce connError
	if errors.As(err, &ce) {
		err = ce.err
	}
	if s, ok := status.FromError(err); ok {
		return s.Code()
	}
	return codes.Unknown
}

// connHint maps well-known transport error texts to an actionable next step.
func connHint(err error) string {
	msg := err.Error()
	switch {
	case strings.Contains(msg, "certificate required"), strings.Contains(msg, "bad certificate"):
		return "the server demands a client certificate (mTLS) — pass --tls-cert / --tls-key"
	case strings.Contains(msg, "unknown authority"):
		return "the certificate isn't signed by a CA this machine trusts — pass --tls-ca (or --tls-skip-verify for dev)"
	case strings.Contains(msg, "deadline exceeded"):
		return "TCP connects but the RPC hangs — an HTTP/1.1-only proxy in the path, or not a gRPC port?"
	}
	return ""
}

func cmdDoctor(args []string) error {
	fs := newFlagSet("doctor", `usage: sparrow doctor [flags]
staged diagnosis of a Flight SQL endpoint: config → dns → tcp → tls → auth →
flight sql → round trip. Names the layer that breaks and shows what the wire
actually presented (TLS version, ALPN, certificate chain).
--server swaps the connection diagnosis for a CONFORMANCE CARD: which Flight
SQL surfaces (GetSqlInfo, catalog RPCs, prepared statements, actions, direct
JSON tickets, IPC compression) the server implements — informational, always
exit 0 once the dial works.
examples: sparrow doctor · sparrow doctor -s influx -o json · sparrow doctor --server`)
	cf := addConnFlags(fs)
	output := fs.String("o", "", `output: "json" for a machine-readable report`)
	serverCard := fs.Bool("server", false, "probe the server's Flight SQL surface instead of the connection")
	parseFlags(fs, args)
	d := &doctor{}
	switch strings.ToLower(*output) {
	case "":
	case "json":
		d.json = true
	default:
		return usagef(`doctor -o supports only "json"`)
	}
	if *serverCard {
		return runConform(cf, d.json)
	}

	t0 := time.Now()
	finish := func() error {
		d.rep.OK = d.fails == 0
		if d.json {
			b, _ := json.MarshalIndent(d.rep, "", "  ")
			fmt.Println(string(b))
		} else {
			fmt.Println()
			line := fmt.Sprintf("%d ok · %d warn · %d fail", d.oks, d.warns, d.fails)
			if d.fails == 0 {
				fmt.Printf("%s — healthy in %d ms\n", line, time.Since(t0).Milliseconds())
			} else {
				fmt.Printf("%s — first failure at %s\n", line, d.firstFail)
			}
		}
		if d.fails > 0 {
			return connError{fmt.Errorf("doctor: %d check(s) failed (first: %s)", d.fails, d.firstFail)}
		}
		return nil
	}
	// stages after config, for skip bookkeeping on early bail
	stages := []string{"dns", "tcp", "tls", "auth", "flightsql", "roundtrip"}
	skipFrom := func(i int) {
		for _, n := range stages[i:] {
			d.emit(checkResult{Check: n, Status: "skip", Detail: "not reached"})
		}
	}

	// 1 · config — profile, URI, key material, config file permissions
	p, pname, err := cf.resolve()
	if err != nil {
		d.emit(checkResult{Check: "config", Status: "fail", Detail: err.Error()})
		skipFrom(0)
		return finish()
	}
	d.rep.Endpoint, d.rep.Profile = p.URI, pname
	if !d.json {
		fmt.Printf("sparrow doctor — %s (profile: %s)\n\n", p.URI, pname)
	}
	target, useTLS, err := parseURI(p.URI)
	if err != nil {
		d.emit(checkResult{Check: "config", Status: "fail", Detail: err.Error()})
		skipFrom(0)
		return finish()
	}
	if _, err := tlsConfigFor(p); err != nil {
		d.emit(checkResult{Check: "config", Status: "fail", Detail: err.Error(),
			Hint: "fix the --tls-cert/--tls-key/--tls-ca paths (or the profile's tls_* fields)"})
		skipFrom(0)
		return finish()
	}
	cfgDetail := fmt.Sprintf("profile %q · auth %s", pname, p.Auth)
	switch {
	case !useTLS:
		cfgDetail += " · plaintext"
	case p.TLSSkipVerify:
		cfgDetail += " · TLS skip-verify"
	case p.TLSCA != "":
		cfgDetail += " · TLS custom CA"
	default:
		cfgDetail += " · TLS system roots"
	}
	if p.TLSCert != "" && useTLS {
		cfgDetail += " + mTLS client cert"
	}
	cfgStatus, cfgHint := "ok", ""
	if runtime.GOOS != "windows" {
		if fi, err := os.Stat(configPath()); err == nil && fi.Mode().Perm()&0o077 != 0 {
			cfgStatus = "warn"
			cfgHint = configPath() + " is group/world-readable and holds credentials — chmod 600"
		}
	}
	d.emit(checkResult{Check: "config", Status: cfgStatus, Detail: cfgDetail, Hint: cfgHint})

	probeTarget := target
	host, _, err := net.SplitHostPort(target)
	if err != nil { // no port in the URI — gRPC assumes 443, so probe 443 too
		host = target
		probeTarget = net.JoinHostPort(target, "443")
		d.emit(checkResult{Check: "config", Status: "warn",
			Detail: "URI has no port — assuming 443 (gRPC's default); prefer an explicit :port"})
	}

	// 2 · dns
	if net.ParseIP(host) != nil {
		d.emit(checkResult{Check: "dns", Status: "skip", Detail: host + " is an IP literal"})
	} else {
		td := time.Now()
		dnsCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		addrs, err := net.DefaultResolver.LookupHost(dnsCtx, host)
		cancel()
		if err != nil {
			d.emit(checkResult{Check: "dns", Status: "fail", Detail: err.Error(),
				Hint: "the name doesn't resolve — typo, or the DNS record hasn't propagated?"})
			skipFrom(1)
			return finish()
		}
		show := addrs
		if len(show) > 3 {
			show = show[:3]
		}
		d.emit(checkResult{Check: "dns", Status: "ok",
			Detail: fmt.Sprintf("%s → %s", host, strings.Join(show, ", ")),
			Ms:     time.Since(td).Milliseconds()})
	}

	// 3 · tcp
	td := time.Now()
	rawConn, err := net.DialTimeout("tcp", probeTarget, 10*time.Second)
	if err != nil {
		d.emit(checkResult{Check: "tcp", Status: "fail", Detail: err.Error(),
			Hint: "nothing accepted the connection — wrong port, service down, or a firewall dropping it"})
		skipFrom(2)
		return finish()
	}
	remote := rawConn.RemoteAddr().String()
	rawConn.Close()
	d.emit(checkResult{Check: "tcp", Status: "ok", Detail: "connected to " + remote,
		Ms: time.Since(td).Milliseconds()})

	// 4 · tls — handshake with the profile's config + ALPN h2; on a verify
	// failure, handshake once more WITHOUT verification purely to report what
	// the wire actually presented (exposes TLS interceptors and wrong certs)
	if !useTLS {
		st, hint := "skip", ""
		det := "plaintext grpc:// — no TLS layer"
		if p.Auth != "none" {
			st = "warn"
			det += "; credentials cross the network unencrypted"
			hint = "use grpc+tls:// if the server offers it"
		}
		d.emit(checkResult{Check: "tls", Status: st, Detail: det, Hint: hint})
	} else {
		tc, _ := tlsConfigFor(p) // validated in the config stage
		tc.NextProtos = []string{"h2"}
		dialer := &net.Dialer{Timeout: 10 * time.Second}
		td = time.Now()
		conn, err := tls.DialWithDialer(dialer, "tcp", probeTarget, tc)
		if err != nil {
			r := checkResult{Check: "tls", Status: "fail", Detail: err.Error(),
				Hint: connHint(err)}
			insecureTC := tc.Clone()
			insecureTC.InsecureSkipVerify = true
			if c2, err2 := tls.DialWithDialer(dialer, "tcp", probeTarget, insecureTC); err2 == nil {
				cs := c2.ConnectionState()
				c2.Close()
				for i, cert := range cs.PeerCertificates {
					if i >= 3 {
						break
					}
					r.Lines = append(r.Lines, "wire presented: "+certLine(cert))
				}
				if strings.Contains(err.Error(), "unknown authority") {
					r.Hint = "if that issuer is not your server's CA, something between you and the " +
						"server is intercepting TLS (antivirus HTTPS scanning, corporate proxy)"
				}
			}
			d.emit(r)
			skipFrom(3)
			return finish()
		}
		cs := conn.ConnectionState()
		conn.Close()
		st, hint := "ok", ""
		var lines []string
		det := tlsVersionName(cs.Version) + " · ALPN " + cs.NegotiatedProtocol
		if len(cs.PeerCertificates) > 0 {
			leaf := cs.PeerCertificates[0]
			days := int(time.Until(leaf.NotAfter).Hours() / 24)
			det += fmt.Sprintf(" · %s (%d days left)", certLine(leaf), days)
			if days < 14 {
				st, hint = "warn", "the server certificate expires soon"
			}
		}
		switch {
		case cs.NegotiatedProtocol != "h2":
			st = "fail"
			hint = "the server didn't negotiate ALPN h2 — gRPC requires it (grpc-go ≥1.67 enforces " +
				"this). Envoy: add alpn_protocols: [h2] to the listener's transport socket."
		case p.TLSSkipVerify:
			st = "warn"
			lines = append(lines, "certificate chain NOT verified (--tls-skip-verify)")
		case interceptorIn(cs.PeerCertificates) != "":
			st = "warn"
			lines = append(lines, "the chain verifies, but the issuer is a TLS interception product ("+
				interceptorIn(cs.PeerCertificates)+") — the server's real certificate is being replaced in transit")
			hint = "antivirus HTTPS scanning or a corporate proxy is decrypting this connection; " +
				"exclude this host from interception to see the server's own certificate"
		}
		d.emit(checkResult{Check: "tls", Status: st, Detail: det, Lines: lines, Hint: hint,
			Ms: time.Since(td).Milliseconds()})
		if st == "fail" {
			skipFrom(3)
			return finish()
		}
	}

	// 5 · auth — dial (basic auth handshakes here) · 6 · flight sql — first RPC
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	td = time.Now()
	cl, ctx, err := dial(ctx, p)
	if err != nil {
		r := checkResult{Check: "auth", Status: "fail", Detail: err.Error(), Hint: connHint(err)}
		if grpcCode(err) == codes.Unauthenticated {
			r.Hint = "the server rejected the credentials — check user/pass or the token"
		}
		d.emit(r)
		skipFrom(4)
		return finish()
	}
	defer cl.Close()
	authMs := time.Since(td).Milliseconds()
	authDetail := map[string]string{
		"basic":  "basic handshake accepted",
		"bearer": "bearer token attached (validated at the first RPC)",
		"none":   "no credentials configured",
	}[p.Auth]

	td = time.Now()
	tinfo, err := cl.GetTables(ctx, &flightsql.GetTablesOpts{})
	var ntables int64
	if err == nil {
		ntables, err = countRows(ctx, cl, tinfo)
	}
	if err != nil {
		if grpcCode(err) == codes.Unauthenticated {
			d.emit(checkResult{Check: "auth", Status: "fail", Detail: err.Error(),
				Hint: "the server rejected the credentials at the first RPC — wrong or expired token?"})
			skipFrom(4)
			return finish()
		}
		d.emit(checkResult{Check: "auth", Status: "ok", Detail: authDetail, Ms: authMs})
		d.emit(checkResult{Check: "flightsql", Status: "fail", Detail: err.Error(),
			Hint: connHint(err)})
		skipFrom(5)
		return finish()
	}
	d.emit(checkResult{Check: "auth", Status: "ok", Detail: authDetail, Ms: authMs})
	vendor := strings.TrimSpace(probeVendor(ctx, cl))
	if vendor == "" {
		vendor = "vendor info unsupported"
	}
	d.emit(checkResult{Check: "flightsql", Status: "ok",
		Detail: fmt.Sprintf("%s · %d tables visible via GetTables", vendor, ntables),
		Ms:     time.Since(td).Milliseconds()})

	// 7 · round trip — no alias on the FROM-less SELECT (Dremio)
	td = time.Now()
	qinfo, err := cl.Execute(ctx, "SELECT 1")
	var nrows int64
	if err == nil {
		nrows, err = countRows(ctx, cl, qinfo)
	}
	if err != nil {
		d.emit(checkResult{Check: "roundtrip", Status: "warn",
			Detail: "GetTables works but SELECT 1 failed: " + err.Error(),
			Hint:   "some servers restrict FROM-less SELECTs — try a real query against a table"})
	} else {
		d.emit(checkResult{Check: "roundtrip", Status: "ok",
			Detail: fmt.Sprintf("SELECT 1 → %d row(s)", nrows),
			Ms:     time.Since(td).Milliseconds()})
	}
	return finish()
}

// countRows drains a FlightInfo's endpoints and counts the rows.
func countRows(ctx context.Context, cl *flightsql.Client, info *flight.FlightInfo) (int64, error) {
	var n int64
	for _, ep := range info.Endpoint {
		rdr, err := cl.DoGet(ctx, ep.Ticket)
		if err != nil {
			return n, err
		}
		for rdr.Next() {
			n += rdr.Record().NumRows()
		}
		err = rdr.Err()
		rdr.Release()
		if err != nil {
			return n, err
		}
	}
	return n, nil
}

// ── ping ────────────────────────────────────────────────────────────────

// percentile: nearest-rank on a sorted slice.
func percentile(sorted []float64, q float64) float64 {
	if len(sorted) == 0 {
		return 0
	}
	return sorted[int(math.Round(q*float64(len(sorted)-1)))]
}

type pingRound struct {
	TCPMs float64 `json:"tcp_ms"`
	RPCMs float64 `json:"rpc_ms"`
	Err   string  `json:"error,omitempty"`
}

// cmdPing measures two round trips per round: a bare TCP connect (pure
// network) and a lightweight RPC on the already-authenticated channel
// (network + server). The gap between their medians is the server's overhead.
func cmdPing(args []string) error {
	fs := newFlagSet("ping", `usage: sparrow ping [flags]
round-trip latency over N rounds: a bare TCP connect (the network) next to a
lightweight RPC on a warm channel (network + server). The gap is the server.
examples: sparrow ping · sparrow ping -n 20 -s gizmo · sparrow ping -o json`)
	cf := addConnFlags(fs)
	n := fs.Int("n", 10, "rounds")
	interval := fs.Duration("interval", 200*time.Millisecond, "pause between rounds")
	output := fs.String("o", "", `output: "json" for a machine-readable report`)
	parseFlags(fs, args)
	asJSON := false
	switch strings.ToLower(*output) {
	case "":
	case "json":
		asJSON = true
	default:
		return usagef(`ping -o supports only "json"`)
	}
	if *n < 1 {
		return usagef("ping -n wants at least 1")
	}

	p, pname, err := cf.resolve()
	if err != nil {
		return err
	}
	target, _, err := parseURI(p.URI)
	if err != nil {
		return connError{err}
	}
	ctx, cancel := context.WithTimeout(context.Background(),
		time.Duration(*n)*(*interval)+2*time.Minute)
	defer cancel()
	cl, ctx, err := dial(ctx, p)
	if err != nil {
		return err
	}
	defer cl.Close()

	if !asJSON {
		fmt.Printf("sparrow ping — %s (profile: %s) · %d rounds\n\n", p.URI, pname, *n)
	}
	pat := "__sparrow_ping__" // matches no table; the RPC round trip is the point
	rounds := make([]pingRound, 0, *n)
	var tcps, rpcs []float64
	fails := 0
	for i := 0; i < *n; i++ {
		if i > 0 {
			time.Sleep(*interval)
		}
		var r pingRound
		t := time.Now()
		c, err := net.DialTimeout("tcp", target, 10*time.Second)
		if err != nil {
			r.Err = err.Error()
		} else {
			r.TCPMs = float64(time.Since(t).Microseconds()) / 1000
			c.Close()
			tcps = append(tcps, r.TCPMs)
			rpcCtx, rc := context.WithTimeout(ctx, 15*time.Second)
			t = time.Now()
			_, err = cl.GetTables(rpcCtx, &flightsql.GetTablesOpts{TableNameFilterPattern: &pat})
			rc()
			if err != nil {
				r.Err = err.Error()
			} else {
				r.RPCMs = float64(time.Since(t).Microseconds()) / 1000
				rpcs = append(rpcs, r.RPCMs)
			}
		}
		if r.Err != "" {
			fails++
		}
		rounds = append(rounds, r)
		if !asJSON {
			if r.Err != "" {
				fmt.Printf("round %2d   FAILED: %s\n", i+1, r.Err)
			} else {
				fmt.Printf("round %2d   tcp %7.1f ms   rpc %7.1f ms\n", i+1, r.TCPMs, r.RPCMs)
			}
		}
	}
	sort.Float64s(tcps)
	sort.Float64s(rpcs)
	quarts := func(v []float64) (min, p50, p95, max float64) {
		if len(v) == 0 {
			return
		}
		return v[0], percentile(v, 0.5), percentile(v, 0.95), v[len(v)-1]
	}
	t1, t2, t3, t4 := quarts(tcps)
	r1, r2, r3, r4 := quarts(rpcs)
	if asJSON {
		out := map[string]any{
			"endpoint": p.URI, "profile": pname, "rounds": rounds, "failures": fails,
			"tcp_ms": map[string]float64{"min": t1, "p50": t2, "p95": t3, "max": t4},
			"rpc_ms": map[string]float64{"min": r1, "p50": r2, "p95": r3, "max": r4},
		}
		b, _ := json.MarshalIndent(out, "", "  ")
		fmt.Println(string(b))
	} else {
		fmt.Println()
		fmt.Printf("        %7s %7s %7s %7s\n", "min", "p50", "p95", "max")
		fmt.Printf("tcp     %7.1f %7.1f %7.1f %7.1f  ms\n", t1, t2, t3, t4)
		fmt.Printf("rpc     %7.1f %7.1f %7.1f %7.1f  ms   (%d/%d ok)\n", r1, r2, r3, r4, len(rpcs), *n)
		if len(tcps) > 0 && len(rpcs) > 0 {
			fmt.Printf("\n≈ network %.1f ms + server %.1f ms (medians)\n", t2, math.Max(0, r2-t2))
		}
	}
	if len(rpcs) == 0 {
		return connError{fmt.Errorf("ping: all %d rounds failed", *n)}
	}
	return nil
}

// grpcDetailRe strips the redundant ". Detail: Failed" tail that gRPC status
// strings carry into otherwise-clean server error messages.
var grpcDetailRe = regexp.MustCompile(`[.\s]*Detail:\s*[A-Za-z]+\s*$`)

func usage() {
	fmt.Println(`sparrow ` + versionString() + ` — Arrow Flight data. At the speed of the command line.

usage:
  sparrow connect <grpc[+tls]://host:port> [--basic user:pass] [--tls-skip-verify] [--name p]
  sparrow orient [-s profile]                     one-shot markdown map: vendor, tables, schemas
  sparrow ls [pattern] [-s profile] [-o format]
  sparrow info <table> [-s profile] [--no-count]
  sparrow sql "SELECT ..." [-s profile] [-o format|file] [--max-rows N] [--stats] [--ipc]
  sparrow sql -                                   read SQL from stdin (also: -f query.sql)
  sparrow sql --substrait plan.pb                 execute a serialized Substrait plan
  sparrow query <table> [--where ..] [--limit N]   build the SELECT for you (sql sugar)
  sparrow head <table> [n]                        preview the first n rows (default 10)
  sparrow pull '<ticket>'                          Direct Pull (1-RTT): a ready ticket straight to the server, no GetFlightInfo
  sparrow ticket "<sql>" | --series a,b            emit a reusable pull ticket (JSON) to save and replay with pull @file
  sparrow profile <table> [-o json]               per-column nulls · distinct · min · max
  sparrow doctor [-s profile] [-o json]           layered diagnosis: config→dns→tcp→tls→auth→sql
  sparrow doctor --server                         conformance card: which Flight SQL surfaces work
  sparrow check <table> [--key c] [--time c]      data doctor: nulls·dupes·staleness·frozen·outliers
  sparrow diff <table> --against <profile|uri>    drift gate: schema·count·bounds vs a second server
  sparrow audit [-s profile] [-o json]            security surface: what client SQL can reach beyond queries
  sparrow ping [-n N] [-s profile] [-o json]      latency: bare TCP vs warm-channel RPC, percentiles
  sparrow feedback "message" [--category bug]     send feedback to the sparrow maintainers
  sparrow profiles [use <name> | rm <name>]
  sparrow completion bash|zsh|fish                shell tab-completion script
  sparrow agent                                   print a complete agent-ready manual (markdown) for driving sparrow
  sparrow version

output (-o): table · csv · json · jsonl · md · arrow — or a file path:
  data.parquet · data.csv · data.json · data.jsonl · data.arrow · data.md
Defaults: a TTY gets a table (numeric columns add a whole-stream sparkline);
a pipe streams raw Arrow IPC (composable with DuckDB, Python, anything).
Agents and scripts: use -o md, -o jsonl or -o csv.
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
	case "query":
		err = cmdQuery(os.Args[2:])
	case "head":
		err = cmdHead(os.Args[2:])
	case "pull", "doget": // "doget" = hidden alias (the DoGet RPC name)
		err = cmdPull(os.Args[2:])
	case "profile":
		err = cmdProfile(os.Args[2:])
	case "check":
		err = cmdCheck(os.Args[2:])
	case "diff":
		err = cmdDiff(os.Args[2:])
	case "audit":
		err = cmdAudit(os.Args[2:])
	case "doctor":
		err = cmdDoctor(os.Args[2:])
	case "ping":
		err = cmdPing(os.Args[2:])
	case "feedback":
		err = cmdFeedback(os.Args[2:])
	case "profiles":
		err = cmdProfiles(os.Args[2:])
	case "completion":
		err = cmdCompletion(os.Args[2:])
	case "agent":
		err = cmdAgent(os.Args[2:])
	case "ticket":
		err = cmdTicket(os.Args[2:])
	case "version":
		fmt.Println("sparrow", versionString())
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
			case "query":
				err = cmdQuery([]string{"-h"})
			case "head":
				err = cmdHead([]string{"-h"})
			case "pull", "doget":
				err = cmdPull([]string{"-h"})
			case "profile":
				err = cmdProfile([]string{"-h"})
			case "check":
				err = cmdCheck([]string{"-h"})
			case "diff":
				err = cmdDiff([]string{"-h"})
			case "audit":
				err = cmdAudit([]string{"-h"})
			case "doctor":
				err = cmdDoctor([]string{"-h"})
			case "ping":
				err = cmdPing([]string{"-h"})
			case "feedback":
				err = cmdFeedback([]string{"-h"})
			case "completion":
				err = cmdCompletion([]string{"-h"})
			case "agent":
				err = cmdAgent([]string{"-h"})
			case "ticket":
				err = cmdTicket([]string{"-h"})
			case "profiles":
				fmt.Println("usage: sparrow profiles              list saved connections (* = default) — or: sparrow agent (agent guide)")
				fmt.Println("       sparrow profiles use <name>   set the default profile")
				fmt.Println("       sparrow profiles rm <name>    remove a profile")
			default:
				usage()
			}
			break
		}
		usage()
	default:
		fmt.Fprintf(os.Stderr, "unknown command %q\n\n", os.Args[1])
		usage()
		os.Exit(3)
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, "error:", grpcDetailRe.ReplaceAllString(err.Error(), ""))
		// connection/auth/profile failures exit 2, query errors exit 1 —
		// gRPC dials lazily, so Unavailable/Unauthenticated surface at the
		// first RPC and are classified here rather than at dial time
		var ue usageError
		if errors.As(err, &ue) {
			os.Exit(3)
		}
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
