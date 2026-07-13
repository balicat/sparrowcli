package main

import (
	"errors"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
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
