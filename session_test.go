package main

import (
	"encoding/json"
	"reflect"
	"testing"
)

func TestRedactArgv(t *testing.T) {
	cases := []struct{ in, want []string }{
		// separate-arg form: value masked, flag kept
		{[]string{"sql", "SELECT 1", "--basic", "user:secret", "-o", "md"},
			[]string{"sql", "SELECT 1", "--basic", "***", "-o", "md"}},
		// = form, single dash, bearer
		{[]string{"pull", "@t.json", "-bearer=tok123"},
			[]string{"pull", "@t.json", "-bearer=***"}},
		{[]string{"sql", "q", "--encrypt-key", "deadbeef", "--header", "database=prod"},
			[]string{"sql", "q", "--encrypt-key", "***", "--header", "***"}},
		// non-sensitive flags and a bare value containing "=" pass through
		{[]string{"sql", "q", "-s", "gizmo", "--where", "a=basic"},
			[]string{"sql", "q", "-s", "gizmo", "--where", "a=basic"}},
		// sensitive flag as the LAST arg (no value to mask) stays as-is
		{[]string{"sql", "q", "--basic"},
			[]string{"sql", "q", "--basic"}},
	}
	for _, c := range cases {
		if got := redactArgv(c.in); !reflect.DeepEqual(got, c.want) {
			t.Errorf("redactArgv(%v) = %v, want %v", c.in, got, c.want)
		}
	}
}

func TestSessionStepRoundTrip(t *testing.T) {
	in := sessionStep{
		TS: "2026-07-21T12:00:00Z", Argv: []string{"sql", "SELECT 1"},
		Endpoint: "grpc+tls://x:443", Kind: "sql", Query: "SELECT 1",
		Rows: 1, Ms: 42,
		FP: &receiptResult{Rows: 1, Columns: []string{"1"}, DigestSum: "7", DigestXor: "7"},
	}
	b, err := json.Marshal(in)
	if err != nil {
		t.Fatal(err)
	}
	var out sessionStep
	if err := json.Unmarshal(b, &out); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(in, out) {
		t.Errorf("round trip lost data: %+v != %+v", out, in)
	}
	// a pull step omits query/fingerprint entirely on the wire
	p := sessionStep{Kind: "pull", Ticket: `{"series":["A"]}`, Rows: 5}
	pb, _ := json.Marshal(p)
	for _, absent := range []string{"query", "fingerprint"} {
		var m map[string]any
		json.Unmarshal(pb, &m)
		if _, has := m[absent]; has {
			t.Errorf("pull step should omit %q: %s", absent, pb)
		}
	}
}
