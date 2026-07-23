package main

import (
	"encoding/json"
	"strings"
	"testing"
)

// drive the server loop in-process: feed newline-delimited JSON-RPC, collect
// the response frames. No network — only protocol paths that don't dial.
func mcpSession(t *testing.T, lines ...string) []map[string]any {
	t.Helper()
	srv := &mcpServer{profile: Profile{URI: "grpc://test:1", Auth: "none"}, pname: "test", defaultCap: 200}
	var out strings.Builder
	if err := srv.serve(strings.NewReader(strings.Join(lines, "\n")+"\n"), &out); err != nil {
		t.Fatalf("serve: %v", err)
	}
	var frames []map[string]any
	for _, ln := range strings.Split(strings.TrimSpace(out.String()), "\n") {
		if ln == "" {
			continue
		}
		var f map[string]any
		if err := json.Unmarshal([]byte(ln), &f); err != nil {
			t.Fatalf("response frame is not JSON: %q (%v)", ln, err)
		}
		frames = append(frames, f)
	}
	return frames
}

func TestMCPInitializeAndToolsList(t *testing.T) {
	frames := mcpSession(t,
		`{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2025-06-18","capabilities":{},"clientInfo":{"name":"t","version":"0"}}}`,
		`{"jsonrpc":"2.0","method":"notifications/initialized"}`,
		`{"jsonrpc":"2.0","id":2,"method":"tools/list"}`,
		`{"jsonrpc":"2.0","id":3,"method":"ping"}`,
	)
	if len(frames) != 3 { // the notification must NOT get a response
		t.Fatalf("want 3 response frames (init, list, ping), got %d: %v", len(frames), frames)
	}
	init := frames[0]["result"].(map[string]any)
	if init["protocolVersion"] != "2025-06-18" {
		t.Errorf("protocolVersion not echoed: %v", init["protocolVersion"])
	}
	caps := init["capabilities"].(map[string]any)
	for _, c := range []string{"tools", "resources", "prompts"} {
		if _, ok := caps[c]; !ok {
			t.Errorf("capability %q not advertised", c)
		}
	}
	si := init["serverInfo"].(map[string]any)
	if si["name"] != "sparrow" || si["version"] != version {
		t.Errorf("serverInfo wrong: %v", si)
	}
	tools := frames[1]["result"].(map[string]any)["tools"].([]any)
	if len(tools) != 11 {
		t.Fatalf("want 11 tools, got %d", len(tools))
	}
	for _, tl := range tools {
		m := tl.(map[string]any)
		name := m["name"].(string)
		// anti-drift pin: every MCP tool is a CLI command, and its description
		// embeds the command's cmdDesc line from completion.go
		desc, ok := cmdDesc[name]
		if !ok {
			t.Errorf("MCP tool %q is not a CLI command in cmdDesc", name)
			continue
		}
		if !strings.Contains(m["description"].(string), desc) {
			t.Errorf("tool %q description does not embed cmdDesc (%q)", name, desc)
		}
		if _, ok := m["inputSchema"].(map[string]any); !ok {
			t.Errorf("tool %q has no inputSchema", name)
		}
		// wish #1: every tool annotated; HONESTLY — feedback SENDS a message,
		// so it must NOT claim readOnlyHint (that's what lets hosts auto-
		// approve the rest)
		ann, ok := m["annotations"].(map[string]any)
		if !ok {
			t.Errorf("tool %q has no annotations", name)
			continue
		}
		wantReadOnly := name != "feedback"
		if ann["readOnlyHint"] != wantReadOnly {
			t.Errorf("tool %q readOnlyHint = %v, want %v", name, ann["readOnlyHint"], wantReadOnly)
		}
		if ann["title"] == nil {
			t.Errorf("tool %q has no title annotation", name)
		}
	}
	if _, ok := frames[2]["result"]; !ok {
		t.Errorf("ping got no result: %v", frames[2])
	}
}

func TestMCPErrors(t *testing.T) {
	frames := mcpSession(t,
		`this is not json`,
		`{"jsonrpc":"2.0","id":10,"method":"no/such/method"}`,
		`{"jsonrpc":"2.0","method":"notifications/unknown"}`,
		`{"jsonrpc":"2.0","id":11,"method":"tools/call","params":{"name":"no-such-tool","arguments":{}}}`,
		`{"jsonrpc":"2.0","id":12,"method":"tools/call","params":{"name":"sql","arguments":{}}}`,
		`{"jsonrpc":"2.0","id":13,"method":"tools/call","params":{"name":"feedback","arguments":{}}}`,
		`{"jsonrpc":"2.0","id":14,"method":"tools/call","params":{"name":"version","arguments":{}}}`,
	)
	if len(frames) != 6 { // parse, method-not-found, unknown tool, sql/feedback missing args, version ok
		t.Fatalf("want 6 response frames, got %d: %v", len(frames), frames)
	}
	code := func(f map[string]any) float64 {
		e, ok := f["error"].(map[string]any)
		if !ok {
			t.Fatalf("expected an error frame: %v", f)
		}
		return e["code"].(float64)
	}
	if code(frames[0]) != -32700 {
		t.Errorf("parse error code: %v", frames[0])
	}
	if code(frames[1]) != -32601 {
		t.Errorf("method-not-found code: %v", frames[1])
	}
	if code(frames[2]) != -32602 {
		t.Errorf("unknown tool must be a protocol error: %v", frames[2])
	}
	// a KNOWN tool with bad arguments is a TOOL error (isError result), not
	// a protocol error — the model can read it and correct itself
	res, ok := frames[3]["result"].(map[string]any)
	if !ok || res["isError"] != true {
		t.Errorf("sql with no query must be isError result, got: %v", frames[3])
	}
	// feedback with no message errors BEFORE any network send
	res, ok = frames[4]["result"].(map[string]any)
	if !ok || res["isError"] != true {
		t.Errorf("feedback with no message must be isError result, got: %v", frames[4])
	}
	// version is pure client-side — works with no reachable server
	res, ok = frames[5]["result"].(map[string]any)
	if !ok || res["isError"] == true {
		t.Fatalf("version must succeed offline, got: %v", frames[5])
	}
	vtext := res["content"].([]any)[0].(map[string]any)["text"].(string)
	if !strings.Contains(vtext, version) || !strings.Contains(vtext, "grpc://test:1") {
		t.Errorf("version text missing version/binding: %q", vtext)
	}
}

// prompts and the version tool's structuredContent are client-side — fully
// testable offline.
func TestMCPPromptsAndStructured(t *testing.T) {
	frames := mcpSession(t,
		`{"jsonrpc":"2.0","id":1,"method":"prompts/list"}`,
		`{"jsonrpc":"2.0","id":2,"method":"prompts/get","params":{"name":"quality-gate","arguments":{"table":"series_data"}}}`,
		`{"jsonrpc":"2.0","id":3,"method":"prompts/get","params":{"name":"nope"}}`,
		`{"jsonrpc":"2.0","id":4,"method":"tools/call","params":{"name":"version","arguments":{}}}`,
	)
	if len(frames) != 4 {
		t.Fatalf("want 4 frames, got %d", len(frames))
	}
	prompts := frames[0]["result"].(map[string]any)["prompts"].([]any)
	if len(prompts) != 2 {
		t.Fatalf("want 2 prompts, got %d", len(prompts))
	}
	msgs := frames[1]["result"].(map[string]any)["messages"].([]any)
	txt := msgs[0].(map[string]any)["content"].(map[string]any)["text"].(string)
	if !strings.Contains(txt, "series_data") {
		t.Errorf("prompt argument not substituted: %q", txt)
	}
	if _, ok := frames[2]["error"]; !ok {
		t.Errorf("unknown prompt must error: %v", frames[2])
	}
	sc, ok := frames[3]["result"].(map[string]any)["structuredContent"].(map[string]any)
	if !ok || sc["version"] != version {
		t.Errorf("version structuredContent missing/wrong: %v", frames[3])
	}
}

// JSON-RPC edges from the tester's protocol round: batch arrays are Invalid
// Request (-32600, not parse error); a methodless request is -32600; a
// notification-form request of ANY method gets NO answer; id:null is a real
// (answerable) id — only a MISSING id makes a notification.
func TestMCPProtocolEdges(t *testing.T) {
	frames := mcpSession(t,
		`[{"jsonrpc":"2.0","id":1,"method":"ping"}]`,
		`{"jsonrpc":"2.0","id":9}`,
		`{"jsonrpc":"2.0","method":"tools/list"}`,
		`{"jsonrpc":"2.0","id":null,"method":"ping"}`,
	)
	if len(frames) != 3 { // batch err, missing-method err, null-id ping result
		t.Fatalf("want 3 frames, got %d: %v", len(frames), frames)
	}
	if e := frames[0]["error"].(map[string]any); e["code"].(float64) != -32600 {
		t.Errorf("batch must be -32600: %v", frames[0])
	}
	if e := frames[1]["error"].(map[string]any); e["code"].(float64) != -32600 {
		t.Errorf("missing method must be -32600: %v", frames[1])
	}
	if _, ok := frames[2]["result"]; !ok || frames[2]["id"] != nil {
		t.Errorf("id:null ping must be answered with id null: %v", frames[2])
	}
}

// MCP-2: assertion values arrive as whatever JSON type the model chose.
func TestFlexibleArgs(t *testing.T) {
	var s scalarArg
	for in, want := range map[string]string{`"x"`: "x", `10217`: "10217", `3.5`: "3.5"} {
		if err := json.Unmarshal([]byte(in), &s); err != nil || s.v == nil || *s.v != want {
			t.Errorf("scalarArg(%s) = %v, %v (want %q)", in, s.v, err, want)
		}
	}
	if err := json.Unmarshal([]byte(`true`), &s); err == nil {
		t.Error("scalarArg must reject a bool")
	}
	var n intArg
	for in, want := range map[string]int{`10`: 10, `"7"`: 7} {
		if err := json.Unmarshal([]byte(in), &n); err != nil || n.v == nil || *n.v != want {
			t.Errorf("intArg(%s) = %v, %v (want %d)", in, n.v, err, want)
		}
	}
	if err := json.Unmarshal([]byte(`"x"`), &n); err == nil {
		t.Error("intArg must reject a non-numeric string")
	}
}

// a table name that would parse as a FLAG must be rejected before the
// captured command runs — parseFlags os.Exit(3)s on unknown flags, which
// would kill the whole server (the dash guard is load-bearing).
func TestMCPDashGuard(t *testing.T) {
	frames := mcpSession(t,
		`{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"check","arguments":{"table":"--evil"}}}`,
	)
	if len(frames) != 1 {
		t.Fatalf("want 1 frame, got %d", len(frames))
	}
	res, ok := frames[0]["result"].(map[string]any)
	if !ok || res["isError"] != true {
		t.Fatalf("dash-guarded table must be an isError result (and the server must survive): %v", frames[0])
	}
	txt := res["content"].([]any)[0].(map[string]any)["text"].(string)
	if !strings.Contains(txt, "must not start with '-'") {
		t.Errorf("guard message missing: %q", txt)
	}
}
