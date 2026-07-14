# `sparrow doctor --server` — the Flight SQL conformance card

`sparrow doctor` asks *can I reach this server?* The conformance card asks a
different question: **which parts of the Flight SQL surface does it actually
implement?** Every vendor diverges somewhere, and knowing where is what makes
one client portable.

```sh
sparrow doctor --server              # card for the default profile
sparrow doctor --server -s dremio    # card for a named profile
sparrow doctor --server -o json      # machine-readable
```

The card is **informational**: an unsupported surface is a warn (`⚠`), not a
failure, and the exit code is always 0 once the dial succeeds (a dial failure
still exits 2, like everything else).

## The ten probes

| probe | what it proves |
|---|---|
| `GetSqlInfo` | asks for the **full** info block and lints it: entry count, decoded capability flags (SQL / Substrait / transactions / cancel / read-only / bulk-ingest), mistyped timeout codes, non-standard codes |
| `GetTables` | the portable discovery path works; counts tables |
| `GetTables+schema` | `IncludeSchema` returns *real* schema blobs — some servers accept the flag and ship empty ones, forcing clients to fall back to `LIMIT 0` |
| `GetCatalogs` | catalog enumeration |
| `GetDBSchemas` | schema enumeration |
| `GetTableTypes` | table-type enumeration |
| `Prepare` | `CreatePreparedStatement` → `Execute` → `Close` round trip |
| `Execute metadata` | `FlightInfo` declares the result schema up front (clients that need the schema before the stream care), endpoint count |
| `SELECT version()` | the dialect answers a version query |
| `ListActions` | custom action discovery; lists what's advertised |

## The GetSqlInfo probe is a conformance linter

Credit where due: an agent test session probed `GetSqlInfo` raw (sparrow
couldn't show it yet), found the public demo server returning only 5
entries with the spec's int32 timeout codes carrying description strings —
and proposed that the card should *lint* the block, pointing at sparrow's
own reference server first. It does now:

```
 ⚠ GetSqlInfo         5 entries — minimal (154 ms)
                      ⚠ no capability flags advertised (SQL / Substrait / transactions / cancel)
                      ⚠ code 100 (STATEMENT_TIMEOUT) expects int32, got string_value "EnergyScope Flight SQL"
                      ⚠ code 101 (TRANSACTION_TIMEOUT) expects int32, got string_value "\""
                      ⚠ code 102 is not a standard SqlInfo code ("DuckDB")
```

A healthy block decodes into a one-line capability summary instead:

```
 ✓ GetSqlInfo         85 entries · SQL ✓ · Substrait ✗ · txns ✓ · cancel ✓ · read-only ✗ · bulk-ingest ✓ (1 ms)
```

(The demo server's block is fixed in its next deploy — the card flagged our
own server first, which is exactly the job.)

## A real capture

```
sparrow doctor --server — grpc+tls://flight.sparrowflight.io:443 (profile: demo)
vendor: EnergyScope 1.0

 ⚠ GetSqlInfo         5 entries — minimal (156 ms)
 ✓ GetTables          2 tables listed (153 ms)
 ✓ GetTables+schema   schemas populated (2/2 tables) (200 ms)
 ✓ GetCatalogs        1 catalog(s) (149 ms)
 ✓ GetDBSchemas       1 schema(s) (156 ms)
 ✓ GetTableTypes      1 table type(s) (139 ms)
 ✓ Prepare            prepare → execute → close round trip (291 ms)
 ✓ Execute metadata   FlightInfo declares the schema · 1 endpoint(s) (74 ms)
 ✓ SELECT version()   v1.5.4 (150 ms)
 ⚠ ListActions        unsupported (Unimplemented)

8 supported · 2 unsupported · 0 errored — informational, exit 0
```

## The quirk matrix (2026-07-14, live servers)

Same card, five vendors:

| probe | EnergyScope | GizmoSQL | RoAPI (DataFusion) | Dremio 26 | InfluxDB 3 |
|---|---|---|---|---|---|
| GetSqlInfo | ⚠ 5 entries, linted (server fix queued) | ✓ 85 entries | ✓ | ✓ | ✓ |
| GetTables | ✓ | ✓ | ✓ | ✓ | ✓ |
| GetTables+schema | ✓ | ✓ | ✓ | ✓ | ✓ |
| GetCatalogs | ✓ | ✓ | ✓ | ✓ (0 catalogs) | ✓ |
| GetDBSchemas | ✓ | ✓ | ✓ | ✓ | ✓ |
| GetTableTypes | ✓ | ✓ | ⚠ Unimplemented | ✓ | ✓ |
| Prepare | ✓ | ✓ | ✓ | ✓ | ✓ |
| Execute metadata | ✓ | ✓ | ✓ | ✓ | ✓ |
| SELECT version() | ✓ | ✓ | ✓ | ✓ | ✓ |
| ListActions | ⚠ | ✓ (13 actions) | ✓ (8 actions) | ✓ (8 actions) | ⚠ |

Notes from the runs:

- **Dremio 26 populates `GetTables` schemas** (31/31 in our fixture). Earlier
  Dremio versions shipped empty blobs — the reason this probe exists. If you
  run an older Dremio, expect the `⚠ accepted, but every table_schema blob is
  empty` line.
- `SELECT version()` is the most portable dialect probe we've found: even
  servers that reject `GetSqlInfo` (older Dremio) answer it — and vice versa
  (InfluxDB), which is why the card runs both.
- `ListActions` being unimplemented is common and harmless for querying; it
  only matters if you rely on vendor actions (transactions, cancellation).
