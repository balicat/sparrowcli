// feedback — send a message to the sparrow maintainers. Deliberately
// INDEPENDENT of whatever Flight server you're connected to: it POSTs to
// the fixed receiver at sparrowflight.io, so it works from any server,
// or with no working server at all — which is exactly when you need it.
// Built so AI agents driving this CLI have a way to file what they hit:
// `sparrow feedback "orient chokes on catalog X" --category bug --from claude`.
package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"runtime"
	"strings"
	"time"
)

const feedbackURL = "https://sparrowflight.io/api/feedback"

func cmdFeedback(args []string) error {
	fs := newFlagSet("feedback", `usage: sparrow feedback "your message" [flags]
send feedback to the sparrow maintainers — goes to sparrowflight.io directly,
independent of whichever Flight server you use (SPARROW_FEEDBACK_URL overrides)
examples: sparrow feedback "orient saved my day"
          sparrow feedback "check false-positives on X" --category bug --from claude`)
	category := fs.String("category", "general", "bug | idea | general")
	from := fs.String("from", "", "who this is from (default: $SPARROW_USER, else anonymous)")
	pos := parseFlags(fs, args)
	msg := strings.TrimSpace(strings.Join(pos, " "))
	if msg == "" {
		return usagef(`usage: sparrow feedback "your message" [--category bug|idea|general] [--from name]`)
	}
	user := *from
	if user == "" {
		user = os.Getenv("SPARROW_USER")
	}
	if user == "" {
		user = "anonymous"
	}
	url := os.Getenv("SPARROW_FEEDBACK_URL")
	if url == "" {
		url = feedbackURL
	}

	// which server the user works against is useful context for a report —
	// config only, no credentials, no dialing
	server := ""
	cfg := loadConfig()
	if p, ok := cfg.Profiles[cfg.Default]; ok {
		server = p.URI
	}

	body, _ := json.Marshal(map[string]string{
		"message":        msg,
		"category":       *category,
		"user":           user,
		"client_version": "sparrowcli/" + versionString() + " " + runtime.GOOS + "/" + runtime.GOARCH,
		"server":         server,
	})
	client := &http.Client{Timeout: 15 * time.Second}
	resp, err := client.Post(url, "application/json", bytes.NewReader(body))
	if err != nil {
		return connError{fmt.Errorf("could not reach the feedback receiver: %w", err)}
	}
	defer resp.Body.Close()
	var ack struct {
		OK bool   `json:"ok"`
		TS string `json:"ts"`
	}
	_ = json.NewDecoder(resp.Body).Decode(&ack)
	if resp.StatusCode != 200 || !ack.OK {
		return connError{fmt.Errorf("feedback receiver answered %s", resp.Status)}
	}
	fmt.Printf("✓ feedback delivered (%s) — thank you\n", ack.TS)
	return nil
}
