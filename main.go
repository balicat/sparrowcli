// sparrow — a terminal client for any Arrow Flight / Flight SQL server.
// M0: connect · ls · sql · TTY table / Arrow IPC pipe output.
package main

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/flight"
	"github.com/apache/arrow-go/v18/arrow/flight/flightsql"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
)

const version = "0.0.1-m0"

// ── profiles ────────────────────────────────────────────────────────────

type Profile struct {
	URI           string `json:"uri"`
	Auth          string `json:"auth"` // "basic" | "none"
	User          string `json:"user,omitempty"`
	Pass          string `json:"pass,omitempty"`
	TLSSkipVerify bool   `json:"tls_skip_verify,omitempty"`
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
		return nil, ctx, err
	}
	var creds grpc.DialOption
	if useTLS {
		creds = grpc.WithTransportCredentials(credentials.NewTLS(&tls.Config{
			InsecureSkipVerify: p.TLSSkipVerify, // GizmoSQL ships self-signed by default
		}))
	} else {
		creds = grpc.WithTransportCredentials(insecure.NewCredentials())
	}
	cl, err := flightsql.NewClient(target, nil, nil, creds)
	if err != nil {
		return nil, ctx, err
	}
	if p.Auth == "basic" {
		authCtx, err := cl.Client.AuthenticateBasicToken(ctx, p.User, p.Pass)
		if err != nil {
			cl.Close()
			return nil, ctx, fmt.Errorf("auth failed: %w", err)
		}
		return cl, authCtx, nil
	}
	return cl, ctx, nil
}

// resolveProfile picks the connection: -s <profile|uri>, else the default profile.
func resolveProfile(server, basic string, tlsSkip bool) (Profile, string, error) {
	cfg := loadConfig()
	name := server
	if name == "" {
		name = cfg.Default
	}
	if p, ok := cfg.Profiles[name]; ok && name != "" {
		return p, name, nil
	}
	if strings.Contains(server, "://") { // ad-hoc URI
		p := Profile{URI: server, Auth: "none", TLSSkipVerify: tlsSkip}
		if basic != "" {
			u, pw, _ := strings.Cut(basic, ":")
			p.Auth, p.User, p.Pass = "basic", u, pw
		}
		return p, "(ad-hoc)", nil
	}
	if server == "" && cfg.Default == "" {
		return Profile{}, "", fmt.Errorf("no default profile — run: sparrow connect <uri> [--basic user:pass]")
	}
	return Profile{}, "", fmt.Errorf("unknown profile %q", name)
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
	return col.ValueStr(row)
}

// streamRecords consumes a Flight reader: pretty table on a TTY, Arrow IPC in a pipe.
func streamRecords(rdr *flight.Reader, maxRows int) (int64, error) {
	total := int64(0)
	if stdoutIsTTY() {
		w := tabwriter.NewWriter(os.Stdout, 2, 4, 2, ' ', 0)
		schema := rdr.Schema()
		heads := make([]string, len(schema.Fields()))
		for i, f := range schema.Fields() {
			heads[i] = f.Name
		}
		fmt.Fprintln(w, strings.Join(heads, "\t"))
		printed := 0
		for rdr.Next() {
			rec := rdr.Record()
			for r := 0; r < int(rec.NumRows()); r++ {
				total++
				if printed >= maxRows {
					continue
				}
				vals := make([]string, int(rec.NumCols()))
				for c := 0; c < int(rec.NumCols()); c++ {
					vals[c] = cell(rec.Column(c), r)
				}
				fmt.Fprintln(w, strings.Join(vals, "\t"))
				printed++
			}
		}
		w.Flush()
		if total > int64(printed) {
			fmt.Fprintf(os.Stderr, "… %d rows total (showing %d; pipe or -o to export)\n", total, printed)
		}
		return total, rdr.Err()
	}
	// pipe: raw Arrow IPC stream — composable with DuckDB, Python, anything
	wr := ipc.NewWriter(os.Stdout, ipc.WithSchema(rdr.Schema()))
	defer wr.Close()
	for rdr.Next() {
		rec := rdr.Record()
		total += rec.NumRows()
		if err := wr.Write(rec); err != nil {
			return total, err
		}
	}
	return total, rdr.Err()
}

func consumeInfo(ctx context.Context, cl *flightsql.Client, info *flight.FlightInfo, maxRows int) (int64, error) {
	var total int64
	for _, ep := range info.Endpoint {
		rdr, err := cl.DoGet(ctx, ep.Ticket)
		if err != nil {
			return total, err
		}
		n, err := streamRecords(rdr, maxRows)
		rdr.Release()
		total += n
		if err != nil {
			return total, err
		}
	}
	return total, nil
}

// ── commands ────────────────────────────────────────────────────────────

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
	fs := flag.NewFlagSet("connect", flag.ExitOnError)
	basic := fs.String("basic", "", "user:pass (API key as user is fine)")
	tlsSkip := fs.Bool("tls-skip-verify", false, "accept self-signed TLS certs")
	name := fs.String("name", "default", "profile name")
	pos := parseFlags(fs, args)
	if len(pos) < 1 {
		return fmt.Errorf("usage: sparrow connect <grpc[+tls]://host:port> [--basic user:pass] [--tls-skip-verify] [--name profile]")
	}
	p := Profile{URI: pos[0], Auth: "none", TLSSkipVerify: *tlsSkip}
	if *basic != "" {
		u, pw, _ := strings.Cut(*basic, ":")
		p.Auth, p.User, p.Pass = "basic", u, pw
	}

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	t0 := time.Now()
	cl, ctx, err := dial(ctx, p)
	if err != nil {
		return err
	}
	defer cl.Close()

	// probe: GetSqlInfo names the vendor on most servers; SELECT 1 is the fallback
	// (Dremio errors on GetSqlInfo; never alias a FROM-less SELECT — see dialect-compat.md)
	serverDesc := ""
	var probeErrs []string
	if info, err := cl.GetSqlInfo(ctx, []flightsql.SqlInfo{
		flightsql.SqlInfoFlightSqlServerName, flightsql.SqlInfoFlightSqlServerVersion,
	}); err != nil {
		probeErrs = append(probeErrs, "GetSqlInfo: "+err.Error())
	} else if rdr, err := cl.DoGet(ctx, info.Endpoint[0].Ticket); err != nil {
		probeErrs = append(probeErrs, "GetSqlInfo DoGet: "+err.Error())
	} else {
		vals := []string{}
		for rdr.Next() {
			rec := rdr.Record()
			if rec.NumCols() >= 2 {
				union := rec.Column(1)
				for r := 0; r < int(rec.NumRows()); r++ {
					vals = append(vals, cell(union, r))
				}
			}
		}
		if err := rdr.Err(); err != nil {
			probeErrs = append(probeErrs, "GetSqlInfo read: "+err.Error())
		}
		rdr.Release()
		serverDesc = strings.TrimSpace(strings.Join(vals, " "))
	}
	if serverDesc == "" {
		if info, err := cl.Execute(ctx, "SELECT 1"); err != nil {
			probeErrs = append(probeErrs, "SELECT 1: "+err.Error())
		} else if rdr, err := cl.DoGet(ctx, info.Endpoint[0].Ticket); err != nil {
			probeErrs = append(probeErrs, "SELECT 1 DoGet: "+err.Error())
		} else {
			for rdr.Next() {
			}
			rdr.Release()
			serverDesc = "Flight SQL server (vendor info unsupported)"
		}
	}
	if serverDesc == "" {
		return fmt.Errorf("connected but probes failed:\n  %s", strings.Join(probeErrs, "\n  "))
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
	fs := flag.NewFlagSet("ls", flag.ExitOnError)
	server := fs.String("s", "", "profile name or grpc URI")
	basic := fs.String("basic", "", "user:pass for ad-hoc URIs")
	tlsSkip := fs.Bool("tls-skip-verify", false, "accept self-signed TLS certs")
	pos := parseFlags(fs, args)

	p, _, err := resolveProfile(*server, *basic, *tlsSkip)
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
	_, err = consumeInfo(ctx, cl, info, 1000)
	return err
}

func cmdSQL(args []string) error {
	fs := flag.NewFlagSet("sql", flag.ExitOnError)
	server := fs.String("s", "", "profile name or grpc URI")
	basic := fs.String("basic", "", "user:pass for ad-hoc URIs")
	tlsSkip := fs.Bool("tls-skip-verify", false, "accept self-signed TLS certs")
	maxRows := fs.Int("max-rows", 40, "max rows to print on a TTY")
	pos := parseFlags(fs, args)
	if len(pos) < 1 {
		return fmt.Errorf(`usage: sparrow sql "SELECT ..." [-s profile]`)
	}
	query := strings.Join(pos, " ")

	p, _, err := resolveProfile(*server, *basic, *tlsSkip)
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
	total, err := consumeInfo(ctx, cl, info, *maxRows)
	if err != nil {
		return err
	}
	if stdoutIsTTY() {
		fmt.Fprintf(os.Stderr, "✓ %d rows in %d ms\n", total, time.Since(t0).Milliseconds())
	}
	return nil
}

func cmdProfiles() error {
	cfg := loadConfig()
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
  sparrow ls [pattern] [-s profile]
  sparrow sql "SELECT ..." [-s profile] [--max-rows N]
  sparrow profiles
  sparrow version

On a TTY, results print as a table. In a pipe, results stream as raw Arrow IPC.`)
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
	case "sql":
		err = cmdSQL(os.Args[2:])
	case "profiles":
		err = cmdProfiles()
	case "version":
		fmt.Println("sparrow", version)
	case "help", "-h", "--help":
		usage()
	default:
		usage()
		os.Exit(3)
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}
