// ticket — `sparrow ticket` emits a REUSABLE Sparrow pull ticket (JSON) to
// stdout: the inverse of `sparrow pull`. Save it once, replay it any number of
// times with `sparrow pull @file`, each a 1-RTT Direct Pull.
//
// Why this is the reusable artifact (and a GetFlightInfo handle is not): a
// Flight SQL statement ticket from GetFlightInfo is a server-minted handle,
// consumed on the first DoGet (single-use). A client-constructed ticket is
// stateless — the server re-runs it fresh every DoGet, so it works forever and
// survives restarts (it's just text). This command only formats the JSON; no
// server round trip, so it works offline.
package main

import (
	"encoding/json"
	"fmt"
	"strings"
)

func cmdTicket(args []string) error {
	fs := newFlagSet("ticket", `usage: sparrow ticket "<sql>"   |   sparrow ticket --series ID[,ID,...]
Emit a REUSABLE pull ticket (JSON) to stdout. Save it and replay it any number
of times with `+"`sparrow pull @file`"+` — each a 1-RTT Direct Pull, re-run fresh
server-side (unlike a GetFlightInfo handle, which is single-use). Client-side
only; no server connection needed.
examples:
  sparrow ticket "SELECT period, value FROM series_data WHERE series_id='PET.RWTC.D'" > wti.ticket
  sparrow ticket --series PET.RWTC.D,FRED.DFF --start 2020-01-01 > two.ticket
  sparrow pull @wti.ticket -o md          # reuse it, as often as you like`)
	series := fs.String("series", "", "comma-separated series ids → a {\"series\":[...]} ticket")
	start := fs.String("start", "", "start bound, e.g. 2020-01-01 (series ticket only)")
	end := fs.String("end", "", "end bound (series ticket only)")
	pretty := fs.Bool("pretty", false, "pretty-print the JSON")
	pos := parseFlags(fs, args)

	tk := map[string]any{}
	switch {
	case *series != "":
		if len(pos) > 0 {
			return usagef("ticket: give a SQL string OR --series, not both")
		}
		ids := make([]string, 0, 4)
		for _, s := range strings.Split(*series, ",") {
			if s = strings.TrimSpace(s); s != "" {
				ids = append(ids, s)
			}
		}
		if len(ids) == 0 {
			return usagef("ticket: --series needs at least one id")
		}
		tk["series"] = ids
		if *start != "" {
			tk["start"] = *start
		}
		if *end != "" {
			tk["end"] = *end
		}
	case len(pos) == 1:
		q := strings.TrimSpace(pos[0])
		if q == "" {
			return usagef("ticket: empty SQL")
		}
		tk["sql"] = q
	default:
		return usagef(`usage: sparrow ticket "<sql>"  |  sparrow ticket --series ID[,ID,...]`)
	}

	var out []byte
	var err error
	if *pretty {
		out, err = json.MarshalIndent(tk, "", "  ")
	} else {
		out, err = json.Marshal(tk)
	}
	if err != nil {
		return err
	}
	fmt.Println(string(out))
	return nil
}
