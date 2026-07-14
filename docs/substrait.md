# `sparrow sql --substrait` — execute a Substrait plan over Flight SQL

Flight SQL can carry a query two ways: SQL text (`CommandStatementQuery`)
or a serialized [Substrait](https://substrait.io) plan
(`CommandStatementSubstraitPlan`). sparrow supports both — this implements
the Flight SQL Substrait extension as defined by the Apache Arrow
specification.

```sh
sparrow sql --substrait plan.pb            # execute the plan, stream Arrow back
sparrow sql --substrait plan.pb -o md      # every output format works
sparrow sql --substrait plan.pb --stats    # so does the anatomy
```

sparrow stays a **client, not a planner**: it sends a plan you already
have; producing plans is the job of Ibis, DataFusion, Isthmus, or DuckDB's
substrait extension. Substrait's real audience is software talking to
software — a query builder or dataframe library serializing plans under a
typed API, with no SQL string generation, no dialect differences, no
quoting bugs.

## The capability pre-check

Before firing a plan, sparrow reads the server's advertised Substrait flag
(`GetSqlInfo` code 5):

- **`false`** → refuses with a clear message and exit 1, instead of a raw
  gRPC `Unimplemented`:
  `server advertises Substrait=False (GetSqlInfo code 5) — it will not accept a plan`
- **absent** → warns (`attempting anyway`) and sends it.
- **`true`** → sends it.

`sparrow doctor --server` shows the flag in the capability line —
`flight.sparrowflight.io` advertises `Substrait ✓`.

## A real capture (public endpoint, 2026-07-14)

A 277-byte plan — no SQL text anywhere on the wire — against a
136M-row table:

```
$ sparrow sql --substrait wti_plan.pb -o csv --stats
series_id,period,value
PET.RWTC.D,20260706,69.6
PET.RWTC.D,20260702,69.73
PET.RWTC.D,20260701,69.74
PET.RWTC.D,20260630,70.56
PET.RWTC.D,20260629,71.87
── query stats ─────────────────────────
plan (GetFlightInfo)     164 ms
first byte                81 ms
stream (DoGet)            81 ms
total                    245 ms
```

## Producing plans with DuckDB (and the footgun we hit)

DuckDB's substrait community extension is the quickest producer:

```python
import duckdb
con = duckdb.connect()
con.execute("INSTALL substrait FROM community; LOAD substrait")
# the producer needs the table's SCHEMA — the server resolves the name
con.execute("CREATE TABLE series_data (series_id VARCHAR, period VARCHAR, value DOUBLE)")
plan = con.execute(
    "CALL get_substrait(?, enable_optimizer=false)",
    ["SELECT series_id, period, value FROM series_data WHERE series_id = 'PET.RWTC.D' ORDER BY period DESC LIMIT 5"],
).fetchone()[0]
open("wti_plan.pb", "wb").write(plan)
```

**`enable_optimizer=false` matters.** DuckDB's producer runs its optimizer
before serializing, using the *producer's local statistics*. If your local
`series_data` is an empty schema shell (the natural way to produce plans
for a remote table), the optimizer statically proves any filter matches
nothing and serializes an **empty virtual table** — no table reference, no
filter, nothing. The plan is then "correctly" executed by any consumer as
zero rows. We lost an hour to this: unfiltered scans worked, every
filtered plan returned empty, and `EXPLAIN` on the consumer showed
`EMPTY_RESULT` where the scan should be. Disable the optimizer at
production time (or produce against representative data) and the plan
keeps its `namedTable` + `FilterRel`, which the server executes against
the real data.

## Producer compatibility (found the hard way, by an external tester)

Substrait deprecated `FetchRel`'s integer `count`/`offset` fields in favor
of `count_expr`/`offset_expr` (Expression). **DataFusion (Substrait ≥0.85)
emits the new fields; DuckDB's consumer (0.78) reads only the deprecated
ints** — so a DataFusion plan with a `LIMIT` used to execute as `LIMIT 0`
and return a *silently empty* result. Credit: an external tester
root-caused this by patching plan bytes to rule out version and
`maintainSingularStruct`, isolating `FetchRel` as the sole cause.

The sparrowflight.io server now **detects the encoding and fails loudly**
instead:

```
error: Substrait plan encodes LIMIT/OFFSET as FetchRel count_expr/offset_expr
(Substrait >= 0.8x, e.g. DataFusion); this server's consumer (DuckDB substrait,
0.78) reads only the deprecated integer fields and would silently execute
LIMIT 0. Re-produce the plan with a producer that emits the deprecated fetch
fields (e.g. DuckDB get_substrait), or drop the LIMIT/OFFSET from the plan.
```

Practical producer matrix against a DuckDB-consuming server:

| producer | plans without LIMIT | plans with LIMIT |
|---|---|---|
| DuckDB `get_substrait` (`enable_optimizer=false`) | ✓ | ✓ (deprecated int `count`) |
| DataFusion / datafusion-python | ✓ (filters, projections work) | ✗ refused loudly (`count_expr`) |

Known second gap: DataFusion `COUNT(*)` aggregate plans hit a DuckDB
binder error (`SELECT list is empty after resolving * expressions`) — a
separate aggregate-encoding difference, loud rather than silent, unfixed.
The durable fix for both is upstream: DuckDB's consumer reading the
non-deprecated fields.

## Server support

Very few Flight SQL servers accept plans today — `doctor --server` across
five vendors shows exactly one `Substrait ✓` (this project's own serving
node, which consumes plans via DuckDB's substrait extension and advertises
the capability honestly: if the extension fails to load, the flag reads
`False` and plans are refused with a clear error).
