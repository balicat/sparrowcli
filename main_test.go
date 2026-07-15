package main

import (
	"bytes"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"math/big"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

func TestParseURI(t *testing.T) {
	cases := []struct {
		uri     string
		target  string
		tls     bool
		wantErr bool
	}{
		{"grpc://host:1234", "host:1234", false, false},
		{"grpc+tls://host:443", "host:443", true, false},
		{"grpcs://host:443", "host:443", true, false},
		{"http://host:80", "", false, true},
		{"host:1234", "", false, true},
	}
	for _, c := range cases {
		target, useTLS, err := parseURI(c.uri)
		if (err != nil) != c.wantErr {
			t.Errorf("parseURI(%q) err = %v, wantErr %v", c.uri, err, c.wantErr)
			continue
		}
		if err == nil && (target != c.target || useTLS != c.tls) {
			t.Errorf("parseURI(%q) = %q,%v want %q,%v", c.uri, target, useTLS, c.target, c.tls)
		}
	}
}

func TestResolveSink(t *testing.T) {
	for _, f := range []string{"table", "csv", "json", "jsonl", "md", "arrow"} {
		s, err := resolveSink(f)
		if err != nil || s.format != f || s.path != "" {
			t.Errorf("resolveSink(%q) = %+v, %v", f, s, err)
		}
	}
	if s, err := resolveSink("markdown"); err != nil || s.format != "md" {
		t.Errorf("markdown alias: %+v, %v", s, err)
	}

	dir := t.TempDir()
	fileCases := map[string]string{
		"x.parquet": "parquet", "x.pq": "parquet", "x.csv": "csv",
		"x.json": "json", "x.jsonl": "jsonl", "x.ndjson": "jsonl",
		"x.arrow": "arrow", "x.ipc": "arrow", "x.md": "md",
	}
	for name, format := range fileCases {
		p := filepath.Join(dir, name)
		s, err := resolveSink(p)
		if err != nil || s.format != format || s.path != p || s.closer == nil {
			t.Errorf("resolveSink(%q) = %+v, %v (want format %s)", name, s, err, format)
			continue
		}
		s.closer.Close()
	}

	for _, bad := range []string{"bogus", filepath.Join(dir, "x.xyz")} {
		_, err := resolveSink(bad)
		var ue usageError
		if err == nil || !errors.As(err, &ue) {
			t.Errorf("resolveSink(%q) = %v, want usageError", bad, err)
		}
	}
}

func TestAutoMaxRows(t *testing.T) {
	cases := []struct {
		flag   int
		format string
		toFile bool
		want   int
	}{
		{-1, "table", false, 40},
		{-1, "md", false, 1000},
		{-1, "md", true, 0}, // explicit file sink: never capped
		{-1, "csv", false, 0},
		{-1, "arrow", false, 0},
		{5, "md", false, 5}, // explicit flag always wins
		{5, "csv", true, 5},
		{0, "md", false, 0},
	}
	for _, c := range cases {
		if got := autoMaxRows(c.flag, c.format, 40, c.toFile); got != c.want {
			t.Errorf("autoMaxRows(%d,%q,40,%v) = %d, want %d", c.flag, c.format, c.toFile, got, c.want)
		}
	}
}

func TestLoadKey(t *testing.T) {
	hex32 := strings.Repeat("ab", 16) // 16 bytes
	if k, err := loadKey(hex32); err != nil || len(k) != 16 {
		t.Errorf("hex 16B: %v, %v", k, err)
	}
	if k, err := loadKey(strings.Repeat("ab", 32)); err != nil || len(k) != 32 {
		t.Errorf("hex 32B: %v, %v", k, err)
	}
	if _, err := loadKey("zz"); err == nil {
		t.Error("bad hex accepted")
	}
	if _, err := loadKey("abcd"); err == nil {
		t.Error("2-byte key accepted")
	}

	t.Setenv("SPARROW_TEST_KEY", hex32)
	if k, err := loadKey("env:SPARROW_TEST_KEY"); err != nil || len(k) != 16 {
		t.Errorf("env key: %v, %v", k, err)
	}
	if _, err := loadKey("env:SPARROW_TEST_KEY_UNSET"); err == nil {
		t.Error("empty env accepted")
	}

	dir := t.TempDir()
	raw := filepath.Join(dir, "raw.key")
	os.WriteFile(raw, bytes.Repeat([]byte{0x42}, 24), 0o600)
	if k, err := loadKey("file:" + raw); err != nil || len(k) != 24 {
		t.Errorf("raw file key: %v, %v", k, err)
	}
	hexFile := filepath.Join(dir, "hex.key")
	os.WriteFile(hexFile, []byte(hex32+"\n"), 0o600)
	if k, err := loadKey("file:" + hexFile); err != nil || len(k) != 16 {
		t.Errorf("hex file key: %v, %v", k, err)
	}
}

func TestParseHeaders(t *testing.T) {
	if h, err := parseHeaders(nil); err != nil || h != nil {
		t.Errorf("empty: %v, %v", h, err)
	}
	h, err := parseHeaders([]string{"database=mydb", " k = v "})
	if err != nil || h["database"] != "mydb" || h["k"] != "v" {
		t.Errorf("parse: %v, %v", h, err)
	}
	if _, err := parseHeaders([]string{"noequals"}); err == nil {
		t.Error("missing = accepted")
	}
}

func TestGroupDigits(t *testing.T) {
	cases := map[string]string{
		"":          "",
		"1":         "1",
		"123":       "123",
		"1234":      "1,234",
		"1234567":   "1,234,567",
		"136052269": "136,052,269",
	}
	for in, want := range cases {
		if got := groupDigits(in); got != want {
			t.Errorf("groupDigits(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestPercentile(t *testing.T) {
	if got := percentile(nil, 0.5); got != 0 {
		t.Errorf("empty: %v", got)
	}
	one := []float64{7}
	for _, q := range []float64{0, 0.5, 0.95, 1} {
		if got := percentile(one, q); got != 7 {
			t.Errorf("single q=%v: %v", q, got)
		}
	}
	v := []float64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}
	if got := percentile(v, 0); got != 1 {
		t.Errorf("p0: %v", got)
	}
	if got := percentile(v, 1); got != 10 {
		t.Errorf("p100: %v", got)
	}
	if got := percentile(v, 0.5); got != 6 { // nearest-rank on 0..9: round(4.5)=5 → v[5]
		t.Errorf("p50: %v", got)
	}
}

func TestMdEscape(t *testing.T) {
	got := mdEscape([]string{"a|b", "x\ny", "plain"})
	if got[0] != `a\|b` || got[1] != "x y" || got[2] != "plain" {
		t.Errorf("mdEscape: %q", got)
	}
}

func TestConnHint(t *testing.T) {
	cases := map[string]string{
		"remote error: tls: certificate required":       "client certificate",
		"x509: certificate signed by unknown authority": "isn't signed by a CA",
		"context deadline exceeded":                     "RPC hangs",
		"some other error":                              "",
	}
	for msg, want := range cases {
		got := connHint(fmt.Errorf("%s", msg))
		if want == "" && got != "" {
			t.Errorf("connHint(%q) = %q, want none", msg, got)
		}
		if want != "" && !strings.Contains(got, want) {
			t.Errorf("connHint(%q) = %q, want contains %q", msg, got, want)
		}
	}
}

func TestInterceptorIn(t *testing.T) {
	norton := &x509.Certificate{Issuer: pkix.Name{CommonName: "Norton Web/Mail Shield Root",
		Organization: []string{"Norton Web/Mail Shield"}}}
	clean := &x509.Certificate{Issuer: pkix.Name{CommonName: "R11",
		Organization: []string{"Let's Encrypt"}}}
	if got := interceptorIn([]*x509.Certificate{clean, norton}); got == "" {
		t.Error("Norton chain not flagged")
	}
	if got := interceptorIn([]*x509.Certificate{clean}); got != "" {
		t.Errorf("clean chain flagged: %q", got)
	}
}

func genCertPEM(t *testing.T, cn string) []byte {
	t.Helper()
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	tmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: cn},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour),
		IsCA:                  true,
		BasicConstraintsValid: true,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, pub, priv)
	if err != nil {
		t.Fatal(err)
	}
	return pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
}

func TestTLSConfigFor(t *testing.T) {
	tc, err := tlsConfigFor(Profile{TLSSkipVerify: true})
	if err != nil || !tc.InsecureSkipVerify {
		t.Errorf("skip-verify: %+v, %v", tc, err)
	}
	if _, err := tlsConfigFor(Profile{TLSCert: "nope.crt", TLSKey: "nope.key"}); err == nil {
		t.Error("missing keypair accepted")
	}

	dir := t.TempDir()
	ca := filepath.Join(dir, "ca.crt")
	os.WriteFile(ca, genCertPEM(t, "Test CA"), 0o600)
	tc, err = tlsConfigFor(Profile{TLSCA: ca})
	if err != nil || tc.RootCAs == nil {
		t.Errorf("custom CA: %+v, %v", tc, err)
	}
	garbage := filepath.Join(dir, "garbage.crt")
	os.WriteFile(garbage, []byte("not a pem"), 0o600)
	if _, err := tlsConfigFor(Profile{TLSCA: garbage}); err == nil {
		t.Error("garbage CA accepted")
	}
}

// testRecord builds (id int64, name utf8 nullable) with rows (1,"a") (2,NULL) (3,"p|pe").
func testRecord(t *testing.T) arrow.Record {
	t.Helper()
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
		{Name: "name", Type: arrow.BinaryTypes.String, Nullable: true},
	}, nil)
	b := array.NewRecordBuilder(memory.DefaultAllocator, schema)
	defer b.Release()
	b.Field(0).(*array.Int64Builder).AppendValues([]int64{1, 2, 3}, nil)
	nb := b.Field(1).(*array.StringBuilder)
	nb.Append("a")
	nb.AppendNull()
	nb.Append("p|pe")
	return b.NewRecord()
}

func writeVia(t *testing.T, format string, maxRows int, rec arrow.Record) (string, *recordWriter) {
	t.Helper()
	buf := &bytes.Buffer{}
	rw := &recordWriter{s: sink{format: format, w: buf}, maxRows: maxRows}
	if err := rw.begin(rec.Schema()); err != nil {
		t.Fatal(err)
	}
	if err := rw.write(rec); err != nil {
		t.Fatal(err)
	}
	if err := rw.end(); err != nil {
		t.Fatal(err)
	}
	return buf.String(), rw
}

func TestRecordWriterCSV(t *testing.T) {
	rec := testRecord(t)
	defer rec.Release()
	out, _ := writeVia(t, "csv", 0, rec)
	lines := strings.Split(strings.TrimSpace(out), "\n")
	if len(lines) != 4 || lines[0] != "id,name" {
		t.Fatalf("csv: %q", out)
	}
	if lines[2] != "2," { // NULL must be an empty cell, not the string "NULL"
		t.Errorf("csv NULL row: %q", lines[2])
	}
}

func TestRecordWriterMd(t *testing.T) {
	rec := testRecord(t)
	defer rec.Release()
	out, _ := writeVia(t, "md", 0, rec)
	if !strings.Contains(out, `p\|pe`) {
		t.Errorf("md pipe not escaped: %q", out)
	}
	if !strings.HasPrefix(out, "| id | name |") {
		t.Errorf("md header: %q", out)
	}
}

func TestRecordWriterJSON(t *testing.T) {
	rec := testRecord(t)
	defer rec.Release()
	out, _ := writeVia(t, "json", 0, rec)
	var rows []map[string]any
	if err := json.Unmarshal([]byte(out), &rows); err != nil || len(rows) != 3 {
		t.Fatalf("json: %v rows=%d out=%q", err, len(rows), out)
	}
	if rows[1]["name"] != nil {
		t.Errorf("json NULL: %v", rows[1])
	}

	out, _ = writeVia(t, "jsonl", 0, rec)
	if n := len(strings.Split(strings.TrimSpace(out), "\n")); n != 3 {
		t.Errorf("jsonl lines: %d, %q", n, out)
	}
}

func TestRecordWriterJSONEmpty(t *testing.T) {
	rec := testRecord(t)
	defer rec.Release()
	buf := &bytes.Buffer{}
	rw := &recordWriter{s: sink{format: "json", w: buf}}
	if err := rw.begin(rec.Schema()); err != nil {
		t.Fatal(err)
	}
	if err := rw.end(); err != nil {
		t.Fatal(err)
	}
	var rows []any
	if err := json.Unmarshal(buf.Bytes(), &rows); err != nil || len(rows) != 0 {
		t.Errorf("empty json: %q, %v", buf.String(), err)
	}
}

func TestRecordWriterMaxRows(t *testing.T) {
	rec := testRecord(t)
	defer rec.Release()
	out, rw := writeVia(t, "csv", 2, rec)
	if rw.total != 3 || rw.written != 2 {
		t.Errorf("counts: total=%d written=%d", rw.total, rw.written)
	}
	if lines := strings.Split(strings.TrimSpace(out), "\n"); len(lines) != 3 { // header + 2
		t.Errorf("capped csv: %q", out)
	}
}

func TestCell(t *testing.T) {
	rec := testRecord(t)
	defer rec.Release()
	if got := cell(rec.Column(0), 0); got != "1" {
		t.Errorf("int cell: %q", got)
	}
	if got := cell(rec.Column(1), 1); got != "NULL" {
		t.Errorf("null cell: %q", got)
	}
}

func TestCellDenseUnion(t *testing.T) {
	dt := arrow.DenseUnionOf([]arrow.Field{
		{Name: "i", Type: arrow.PrimitiveTypes.Int64},
		{Name: "s", Type: arrow.BinaryTypes.String},
	}, []arrow.UnionTypeCode{0, 1})
	ub := array.NewDenseUnionBuilder(memory.DefaultAllocator, dt)
	defer ub.Release()
	ub.Append(0)
	ub.Child(0).(*array.Int64Builder).Append(42)
	ub.Append(1)
	ub.Child(1).(*array.StringBuilder).Append("hello")
	u := ub.NewArray()
	defer u.Release()
	// GetSqlInfo values arrive as dense unions; cell must unwrap to the child,
	// not render arrow's [typeid,"value"] form
	if got := cell(u, 0); got != "42" {
		t.Errorf("union int: %q", got)
	}
	if got := cell(u, 1); got != "hello" {
		t.Errorf("union string: %q", got)
	}
}

func TestErrorClassification(t *testing.T) {
	var ue usageError
	if !errors.As(usagef("bad flag"), &ue) {
		t.Error("usagef not a usageError")
	}
	var ce connError
	if !errors.As(connError{errors.New("down")}, &ce) {
		t.Error("connError not classified")
	}
	if errors.As(usagef("x"), &ce) {
		t.Error("usageError classified as connError")
	}
}

func TestSpark(t *testing.T) {
	vals := make([]float64, 100)
	for i := range vals {
		vals[i] = float64(i)
	}
	s := []rune(spark(vals, 50))
	if len(s) != 50 || s[0] != '▁' || s[49] != '█' {
		t.Errorf("ramp: %s", string(s))
	}
	if flat := spark([]float64{5, 5, 5, 5, 5, 5, 5, 5}, 50); flat != strings.Repeat("▄", 8) {
		t.Errorf("flat (short input → own length): %s", flat)
	}
}

func TestRecordWriterSampling(t *testing.T) {
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "v", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
		{Name: "s", Type: arrow.BinaryTypes.String},
	}, nil)
	b := array.NewRecordBuilder(memory.DefaultAllocator, schema)
	defer b.Release()
	vb := b.Field(0).(*array.Int64Builder)
	sb := b.Field(1).(*array.StringBuilder)
	for i := 0; i < 10; i++ {
		if i == 3 {
			vb.AppendNull()
		} else {
			vb.Append(int64(i))
		}
		sb.Append("x")
	}
	rec := b.NewRecord()
	defer rec.Release()

	rw := &recordWriter{sparkCols: []int{0}, samples: make([][]float64, 1), schema: schema}
	rw.sample(rec)
	if len(rw.samples[0]) != 9 { // the null row is skipped
		t.Errorf("samples: %d", len(rw.samples[0]))
	}
	if rw.sparkDone { // cap not reached yet
		t.Error("sparkDone set before cap")
	}
}

func TestCompletionTables(t *testing.T) {
	cmds := completionCommands()
	if len(cmds) < 13 {
		t.Errorf("commands: %v", cmds)
	}
	sql := strings.Join(flagsFor("sql"), " ")
	for _, want := range []string{"--s", "--stats", "--ipc", "--tls-ca"} {
		if !strings.Contains(sql, want) {
			t.Errorf("sql flags missing %s: %s", want, sql)
		}
	}
	if fb := strings.Join(flagsFor("feedback"), " "); strings.Contains(fb, "--tls-ca") {
		t.Errorf("feedback must not take connection flags: %s", fb)
	}
}

func TestReservedWordHint(t *testing.T) {
	perr := errors.New(`Parser Error: syntax error at or near "end"`)
	if h := reservedWordHint("SELECT end FROM t", perr); !strings.Contains(h, `"end"`) {
		t.Errorf("expected end hint, got %q", h)
	}
	// a parser error with no reserved word → no hint
	if h := reservedWordHint("SELECT foo FROM t", perr); h != "" {
		t.Errorf("unexpected hint: %q", h)
	}
	// a non-parser error → no hint even if a reserved word is present
	if h := reservedWordHint("SELECT end FROM t", errors.New("connection refused")); h != "" {
		t.Errorf("hint on non-parser error: %q", h)
	}
}

func TestStatusRank(t *testing.T) {
	if !(statusRank("ok") < statusRank("warn") && statusRank("warn") < statusRank("error") &&
		statusRank("error") < statusRank("fail")) {
		t.Error("status severity order wrong")
	}
	if statusRank("skip") != 0 {
		t.Error("skip should rank 0")
	}
}
