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
	si := init["serverInfo"].(map[string]any)
	if si["name"] != "sparrow" || si["version"] != version {
		t.Errorf("serverInfo wrong: %v", si)
	}
	tools := frames[1]["result"].(map[string]any)["tools"].([]any)
	if len(tools) != 8 {
		t.Fatalf("want 8 tools, got %d", len(tools))
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
