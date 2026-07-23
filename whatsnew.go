// whatsnew — what changed in recent sparrow releases. The content is the
// GitHub releases feed (whose notes goreleaser generates from commit
// subjects), fetched live — no hand-written changelog to drift. Works with no
// Flight server configured; shared by the CLI command and the MCP tool.
package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"
)

const releasesURL = "https://api.github.com/repos/balicat/sparrowcli/releases"

type ghRelease struct {
	Tag       string `json:"tag_name"`
	Name      string `json:"name"`
	Published string `json:"published_at"`
	Body      string `json:"body"`
}

func cmdWhatsnew(args []string) error {
	fs := newFlagSet("whatsnew", `usage: sparrow whatsnew [-n N]
what changed in the last N releases (default 3) — fetched from the GitHub
releases feed, so it is always the shipped truth, not a maintained changelog
example: sparrow whatsnew -n 5`)
	n := fs.Int("n", 3, "how many releases to show")
	parseFlags(fs, args)
	md, err := whatsnewMarkdown(*n)
	if err != nil {
		return err
	}
	fmt.Print(md)
	return nil
}

func whatsnewMarkdown(n int) (string, error) {
	if n < 1 {
		n = 1
	}
	if n > 20 {
		n = 20
	}
	req, err := http.NewRequest("GET", fmt.Sprintf("%s?per_page=%d", releasesURL, n), nil)
	if err != nil {
		return "", err
	}
	req.Header.Set("User-Agent", "sparrowcli/"+versionString())
	req.Header.Set("Accept", "application/vnd.github+json")
	client := &http.Client{Timeout: 15 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return "", connError{fmt.Errorf("could not reach the releases feed: %w", err)}
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		return "", connError{fmt.Errorf("releases feed answered %s", resp.Status)}
	}
	var rels []ghRelease
	if err := json.NewDecoder(resp.Body).Decode(&rels); err != nil {
		return "", fmt.Errorf("releases feed: %s", firstLine(err))
	}
	if len(rels) == 0 {
		return "no releases published yet\n", nil
	}
	var b strings.Builder
	fmt.Fprintf(&b, "running: sparrow %s · latest release: %s\n", versionString(), rels[0].Tag)
	for _, r := range rels {
		date := r.Published
		if len(date) >= 10 {
			date = date[:10]
		}
		fmt.Fprintf(&b, "\n## %s — %s\n\n%s\n", r.Tag, date, strings.TrimSpace(r.Body))
	}
	return b.String(), nil
}
