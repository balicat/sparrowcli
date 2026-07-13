package main

import (
	"bytes"
	"errors"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/ipc"
)

func TestQuoteIdent(t *testing.T) {
	if got := quoteIdent("period"); got != `"period"` {
		t.Errorf("plain: %s", got)
	}
	if got := quoteIdent(`we"ird`); got != `"we""ird"` {
		t.Errorf("embedded quote: %s", got)
	}
}

func TestTableExpr(t *testing.T) {
	if got := tableExpr("series_data"); got != `"series_data"` {
		t.Errorf("simple: %s", got)
	}
	if got := tableExpr("sys.options"); got != "sys.options" {
		t.Errorf("dotted must pass verbatim: %s", got)
	}
	if got := tableExpr(`"already".quoted`); got != `"already".quoted` {
		t.Errorf("pre-quoted must pass verbatim: %s", got)
	}
}

func TestParseAge(t *testing.T) {
	if d, err := parseAge("7d"); err != nil || d != 7*24*time.Hour {
		t.Errorf("7d: %v, %v", d, err)
	}
	if d, err := parseAge("48h"); err != nil || d != 48*time.Hour {
		t.Errorf("48h: %v, %v", d, err)
	}
	if _, err := parseAge("soon"); err == nil {
		t.Error("garbage accepted")
	}
}

func TestParseWhen(t *testing.T) {
	yes := []string{
		"2026-07-11",
		"2026-07-11T00:00:00Z",
		"2026-06-30 12:00:00",
		"2026-01",
		"1859", // annual data renders as a bare year
	}
	for _, s := range yes {
		if _, ok := parseWhen(s); !ok {
			t.Errorf("parseWhen(%q) failed", s)
		}
	}
	no := []string{"2027Q4", "next week", ""}
	for _, s := range no {
		if _, ok := parseWhen(s); ok {
			t.Errorf("parseWhen(%q) unexpectedly parsed", s)
		}
	}
}

func TestSplitCols(t *testing.T) {
	if got := splitCols(" a , b ,,c"); len(got) != 3 || got[0] != "a" || got[2] != "c" {
		t.Errorf("splitCols: %v", got)
	}
	if got := splitCols(""); got != nil {
		t.Errorf("empty: %v", got)
	}
}

func TestTypeClassifiers(t *testing.T) {
	if !isNumericType(arrow.INT64) || !isNumericType(arrow.FLOAT64) || !isNumericType(arrow.DECIMAL128) {
		t.Error("numeric types not recognized")
	}
	if isNumericType(arrow.STRING) || isNumericType(arrow.TIMESTAMP) {
		t.Error("non-numeric classified numeric")
	}
	if !isTemporalType(arrow.TIMESTAMP) || !isTemporalType(arrow.DATE32) {
		t.Error("temporal types not recognized")
	}
	if isTemporalType(arrow.STRING) {
		t.Error("string classified temporal")
	}
}

func TestFirstLine(t *testing.T) {
	if got := firstLine(errors.New("top\ndetail\nmore")); got != "top" {
		t.Errorf("firstLine: %q", got)
	}
	if got := firstLine(errors.New("single")); got != "single" {
		t.Errorf("single: %q", got)
	}
}

func TestTrimFloat(t *testing.T) {
	if got := trimFloat("11130488.832"); got != "11130488.83" {
		t.Errorf("float: %q", got)
	}
	if got := trimFloat("not-a-number"); got != "not-a-number" {
		t.Errorf("passthrough: %q", got)
	}
}

func TestArrayDataSize(t *testing.T) {
	rec := testRecord(t)
	defer rec.Release()
	var n int64
	for _, c := range rec.Columns() {
		n += arrayDataSize(c.Data())
	}
	if n <= 0 {
		t.Fatalf("no bytes counted: %d", n)
	}
	if arrayDataSize(nil) != 0 {
		t.Error("nil interface not guarded")
	}
}

func TestEncodingOf(t *testing.T) {
	cases := map[string]string{
		"utf8":                                   "plain",
		"float64":                                "plain",
		"dictionary<values=utf8, indices=int32>": "dict",
		"run_end_encoded<run_ends: int32, values: utf8>": "ree",
	}
	for typ, want := range cases {
		if got := encodingOf(typ); got != want {
			t.Errorf("encodingOf(%q) = %q, want %q", typ, got, want)
		}
	}
}

func TestFmtBytes(t *testing.T) {
	if got := fmtBytes(22_500_000); got != "22.5 MB" {
		t.Errorf("MB: %q", got)
	}
	if got := fmtBytes(4_200); got != "4 KB" {
		t.Errorf("KB: %q", got)
	}
}

func TestArrayDataSizeTypedNil(t *testing.T) {
	var d *array.Data
	if arrayDataSize(d) != 0 { // typed nil inside the interface — the crash case
		t.Error("typed-nil *array.Data not guarded")
	}
}

// ipcMessages splits an Arrow IPC stream into its message headers
// (continuation marker · metadata length · flatbuffer · padded body).
func ipcMessages(t *testing.T, b []byte) [][]byte {
	t.Helper()
	var out [][]byte
	off := 0
	for off+8 <= len(b) {
		cont, _ := fbU32(b, off)
		if cont != 0xFFFFFFFF {
			break
		}
		ln, _ := fbU32(b, off+4)
		if ln == 0 {
			break // end-of-stream
		}
		hdr := b[off+8 : off+8+int(ln)]
		out = append(out, hdr)
		// Message.bodyLength (field 3) tells us how far to skip
		root, _ := fbU32(hdr, 0)
		var body int64
		if blOff := fbField(hdr, int(root), 3); blOff != 0 && blOff+8 <= len(hdr) {
			for i := 7; i >= 0; i-- {
				body = body<<8 | int64(hdr[blOff+i])
			}
		}
		pad := (8 - body%8) % 8
		off += 8 + int(ln) + int(body+pad)
	}
	return out
}

func TestIpcCodec(t *testing.T) {
	rec := testRecord(t)
	defer rec.Release()

	write := func(opts ...ipc.Option) []byte {
		buf := &bytes.Buffer{}
		w := ipc.NewWriter(buf, append(opts, ipc.WithSchema(rec.Schema()))...)
		if err := w.Write(rec); err != nil {
			t.Fatal(err)
		}
		w.Close()
		return buf.Bytes()
	}
	findBatch := func(stream []byte) (string, bool) {
		for _, h := range ipcMessages(t, stream) {
			if c, isBatch := ipcCodec(h); isBatch {
				return c, true
			}
		}
		return "", false
	}

	if c, ok := findBatch(write()); !ok || c != "" {
		t.Errorf("plain stream: codec %q, batch %v", c, ok)
	}
	if c, ok := findBatch(write(ipc.WithLZ4())); !ok || c != "lz4_frame" {
		t.Errorf("lz4 stream: codec %q, batch %v", c, ok)
	}
	if c, ok := findBatch(write(ipc.WithZstd())); !ok || c != "zstd" {
		t.Errorf("zstd stream: codec %q, batch %v", c, ok)
	}
	// hostile input must not panic
	for _, junk := range [][]byte{nil, {1}, {0xff, 0xff, 0xff}, bytes.Repeat([]byte{0xab}, 64)} {
		ipcCodec(junk)
	}
}
