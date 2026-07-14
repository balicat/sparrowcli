// conform — the Flight SQL conformance card (sparrow doctor --server).
//
// doctor asks "can I reach this server?"; the card asks "which parts of the
// Flight SQL surface does it actually implement?" — the answer differs per
// vendor (GizmoSQL, InfluxDB 3, Dremio, EnergyScope all diverge somewhere)
// and knowing WHICH RPCs work is what makes one client portable. The card is
// informational: unsupported surfaces are warns, not failures, and the exit
// code is always 0 once the dial succeeds.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/flight"
	"github.com/apache/arrow-go/v18/arrow/flight/flightsql"
	"google.golang.org/grpc/codes"
)

type conformReport struct {
	Endpoint    string        `json:"endpoint"`
	Profile     string        `json:"profile"`
	Vendor      string        `json:"vendor,omitempty"`
	Checks      []checkResult `json:"checks"`
	Supported   int           `json:"supported"`
	Unsupported int           `json:"unsupported"`
	Errors      int           `json:"errors"`
}

// conformStatus maps an RPC outcome onto the card's three states.
func conformStatus(err error) string {
	switch {
	case err == nil:
		return "ok"
	case grpcCode(err) == codes.Unimplemented:
		return "warn"
	}
	return "error"
}

func conformDetail(err error) string {
	if grpcCode(err) == codes.Unimplemented {
		return "unsupported (Unimplemented)"
	}
	return firstLine(err)
}

func runConform(cf *connFlags, jsonOut bool) error {
	p, pname, err := cf.resolve()
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	cl, ctx, err := dial(ctx, p)
	if err != nil {
		return connError{err}
	}
	defer cl.Close()

	rep := conformReport{Endpoint: p.URI, Profile: pname}
	rep.Vendor = strings.TrimSpace(probeVendor(ctx, cl))
	if !jsonOut {
		fmt.Printf("sparrow doctor --server — %s (profile: %s)\n", p.URI, pname)
		if rep.Vendor != "" {
			fmt.Printf("vendor: %s\n", rep.Vendor)
		}
		fmt.Println()
	}
	emit := func(name, status, detail string, ms int64) {
		rep.Checks = append(rep.Checks, checkResult{Check: name, Status: status, Detail: detail, Ms: ms})
		switch status {
		case "ok":
			rep.Supported++
		case "warn":
			rep.Unsupported++
		default:
			rep.Errors++
		}
		if !jsonOut {
			line := fmt.Sprintf(" %s %-18s %s", statusMark[status], name, detail)
			if ms > 0 {
				line += fmt.Sprintf(" (%d ms)", ms)
			}
			fmt.Println(line)
		}
	}
	run := func(name string, probe func() (string, error)) {
		t0 := time.Now()
		detail, err := probe()
		if err != nil {
			emit(name, conformStatus(err), conformDetail(err), 0)
			return
		}
		emit(name, "ok", detail, time.Since(t0).Milliseconds())
	}

	run("GetSqlInfo", func() (string, error) {
		info, err := cl.GetSqlInfo(ctx, []flightsql.SqlInfo{
			flightsql.SqlInfoFlightSqlServerName, flightsql.SqlInfoFlightSqlServerVersion,
		})
		if err == nil {
			_, err = countRows(ctx, cl, info)
		}
		if err != nil {
			return "", err
		}
		if rep.Vendor != "" {
			return "server name + version answered", nil
		}
		return "answered (name/version fields empty)", nil
	})

	run("GetTables", func() (string, error) {
		info, err := cl.GetTables(ctx, &flightsql.GetTablesOpts{})
		if err != nil {
			return "", err
		}
		n, err := countRows(ctx, cl, info)
		if err != nil {
			return "", err
		}
		return fmt.Sprintf("%d tables listed", n), nil
	})

	// the Dremio quirk: GetTables accepts IncludeSchema but ships empty blobs
	run("GetTables+schema", func() (string, error) {
		info, err := cl.GetTables(ctx, &flightsql.GetTablesOpts{IncludeSchema: true})
		if err != nil {
			return "", err
		}
		total, withSchema, err := countTableSchemas(ctx, cl, info)
		if err != nil {
			return "", err
		}
		if total > 0 && withSchema == 0 {
			return "", fmt.Errorf("accepted, but every table_schema blob is empty — clients must LIMIT 0 instead")
		}
		return fmt.Sprintf("schemas populated (%d/%d tables)", withSchema, total), nil
	})

	run("GetCatalogs", func() (string, error) {
		info, err := cl.GetCatalogs(ctx)
		if err != nil {
			return "", err
		}
		n, err := countRows(ctx, cl, info)
		if err != nil {
			return "", err
		}
		return fmt.Sprintf("%d catalog(s)", n), nil
	})

	run("GetDBSchemas", func() (string, error) {
		info, err := cl.GetDBSchemas(ctx, &flightsql.GetDBSchemasOpts{})
		if err != nil {
			return "", err
		}
		n, err := countRows(ctx, cl, info)
		if err != nil {
			return "", err
		}
		return fmt.Sprintf("%d schema(s)", n), nil
	})

	run("GetTableTypes", func() (string, error) {
		info, err := cl.GetTableTypes(ctx)
		if err != nil {
			return "", err
		}
		n, err := countRows(ctx, cl, info)
		if err != nil {
			return "", err
		}
		return fmt.Sprintf("%d table type(s)", n), nil
	})

	run("Prepare", func() (string, error) {
		stmt, err := cl.Prepare(ctx, "SELECT 1")
		if err != nil {
			return "", err
		}
		info, err := stmt.Execute(ctx)
		if err != nil {
			stmt.Close(ctx)
			return "", fmt.Errorf("prepared, but Execute failed: %w", err)
		}
		if _, err := countRows(ctx, cl, info); err != nil {
			stmt.Close(ctx)
			return "", err
		}
		if err := stmt.Close(ctx); err != nil {
			return "prepare → execute ok, Close failed (harmless on most servers)", nil
		}
		return "prepare → execute → close round trip", nil
	})

	run("Execute metadata", func() (string, error) {
		info, err := cl.Execute(ctx, "SELECT 1")
		if err != nil {
			return "", err
		}
		eps := len(info.Endpoint)
		if len(info.Schema) == 0 {
			return "", fmt.Errorf("no schema declared in FlightInfo (%d endpoint(s)) — clients must wait for the stream", eps)
		}
		return fmt.Sprintf("FlightInfo declares the schema · %d endpoint(s)", eps), nil
	})

	run("SELECT version()", func() (string, error) {
		info, err := cl.Execute(ctx, "SELECT version()")
		if err != nil {
			return "", err
		}
		v, err := firstCell(ctx, cl, info)
		if err != nil {
			return "", err
		}
		if i := strings.IndexByte(v, '\n'); i > 0 {
			v = v[:i]
		}
		return v, nil
	})

	run("ListActions", func() (string, error) {
		stream, err := cl.Client.ListActions(ctx, &flight.Empty{})
		if err != nil {
			return "", err
		}
		var names []string
		for {
			at, err := stream.Recv()
			if errors.Is(err, io.EOF) {
				break
			}
			if err != nil {
				return "", err
			}
			names = append(names, at.Type)
		}
		if len(names) == 0 {
			return "no custom actions advertised", nil
		}
		shown := names
		if len(shown) > 6 {
			shown = shown[:6]
		}
		return fmt.Sprintf("%d action(s): %s", len(names), strings.Join(shown, ", ")), nil
	})

	if jsonOut {
		b, _ := json.MarshalIndent(rep, "", "  ")
		fmt.Println(string(b))
	} else {
		fmt.Printf("\n%d supported · %d unsupported · %d errored — informational, exit 0\n",
			rep.Supported, rep.Unsupported, rep.Errors)
	}
	return nil
}

// countTableSchemas drains a GetTables(IncludeSchema) result and counts how
// many rows carry a non-empty, deserializable table_schema blob.
func countTableSchemas(ctx context.Context, cl *flightsql.Client, info *flight.FlightInfo) (total, withSchema int64, err error) {
	for _, ep := range info.Endpoint {
		rdr, err := cl.DoGet(ctx, ep.Ticket)
		if err != nil {
			return total, withSchema, err
		}
		for rdr.Next() {
			rec := rdr.Record()
			si := -1
			for i, f := range rec.Schema().Fields() {
				if f.Name == "table_schema" {
					si = i
				}
			}
			total += rec.NumRows()
			if si < 0 {
				continue
			}
			for r := 0; r < int(rec.NumRows()); r++ {
				if rec.Column(si).IsNull(r) {
					continue
				}
				if b := binaryValue(rec.Column(si), r); len(b) > 0 {
					withSchema++
				}
			}
		}
		err = rdr.Err()
		rdr.Release()
		if err != nil {
			return total, withSchema, err
		}
	}
	return total, withSchema, nil
}

// binaryValue extracts the bytes of a binary-typed cell, or nil.
func binaryValue(col arrow.Array, r int) []byte {
	switch c := col.(type) {
	case *array.Binary:
		return c.Value(r)
	case *array.LargeBinary:
		return c.Value(r)
	}
	return nil
}

// firstCell drains a FlightInfo and returns the first row's first column.
func firstCell(ctx context.Context, cl *flightsql.Client, info *flight.FlightInfo) (string, error) {
	out := ""
	for _, ep := range info.Endpoint {
		rdr, err := cl.DoGet(ctx, ep.Ticket)
		if err != nil {
			return "", err
		}
		for rdr.Next() {
			rec := rdr.Record()
			if out == "" && rec.NumRows() > 0 && rec.NumCols() > 0 {
				out = cell(rec.Column(0), 0)
			}
		}
		err = rdr.Err()
		rdr.Release()
		if err != nil {
			return "", err
		}
	}
	return out, nil
}
