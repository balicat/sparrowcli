// feedback — send a message to the server's maintainer over Flight itself.
// The server must implement the "feedback" DoAction (the public
// sparrowflight.io endpoint does: it appends to a JSONL log and notifies
// the maintainer). Built so AI agents driving this CLI have a way to file
// what they hit: `sparrow feedback "orient chokes on catalog X" --category bug`.
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"runtime"
	"strings"
	"time"

	"github.com/apache/arrow-go/v18/arrow/flight"
)

func cmdFeedback(args []string) error {
	fs := newFlagSet("feedback", `usage: sparrow feedback "your message" [flags]
send feedback to the connected server's maintainer (a Flight "feedback"
DoAction — the public sparrowflight.io endpoint accepts it)
examples: sparrow feedback "orient saved my day"
          sparrow feedback "check false-positives on X" --category bug --from claude`)
	cf := addConnFlags(fs)
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

	body, _ := json.Marshal(map[string]string{
		"message":        msg,
		"category":       *category,
		"user":           user,
		"client_version": "sparrowcli/" + versionString() + " " + runtime.GOOS + "/" + runtime.GOARCH,
	})
	stream, err := cl.Client.DoAction(ctx, &flight.Action{Type: "feedback", Body: body})
	if err != nil {
		return fmt.Errorf("this server doesn't accept feedback: %w", err)
	}
	res, err := stream.Recv()
	if err != nil && err != io.EOF {
		return fmt.Errorf("this server doesn't accept feedback: %w", err)
	}
	for err == nil { // drain
		_, err = stream.Recv()
	}
	if res != nil && len(res.Body) > 0 {
		var ack struct {
			OK bool   `json:"ok"`
			TS string `json:"ts"`
		}
		if json.Unmarshal(res.Body, &ack) == nil && ack.OK {
			fmt.Printf("✓ feedback delivered (%s) — thank you\n", ack.TS)
			return nil
		}
	}
	fmt.Println("✓ feedback sent")
	return nil
}
