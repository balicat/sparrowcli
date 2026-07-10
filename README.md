# sparrow

Arrow Flight data. At the speed of the command line.

A single binary that talks to **any Arrow Flight SQL server** — browse the catalog,
run SQL, stream Arrow onward. Human-friendly on a TTY, machine-friendly in a pipe.

Part of the [Sparrow](https://sparrowflight.io) family · spec: [sparrowflight.io/cli](https://sparrowflight.io/cli)

## Usage

```
$ sparrow connect grpc+tls://host:31337 --basic user:pass --tls-skip-verify --name gizmo
✓ connected in 8 ms — gizmosql duckdb v1.5.4
✓ saved profile "gizmo" (default: gizmo)

$ sparrow ls
catalog_name  db_schema_name  table_name  table_type
memory        main            prices      BASE TABLE

$ sparrow sql "SELECT * FROM prices LIMIT 4"
day_no  wti
0       70
1       70.3
2       70.6
3       70.9
✓ 4 rows in 35 ms

# In a pipe, output is a raw Arrow IPC stream — composable with anything:
$ sparrow sql "SELECT * FROM prices" > prices.arrows
$ python -c "import pyarrow.ipc as ipc; print(ipc.open_stream('prices.arrows').read_all())"
```

## Status: M0

- `connect` — header/handshake Basic auth, TLS with `--tls-skip-verify` for
  self-signed certs, vendor probe (GetSqlInfo with `SELECT 1` fallback), saved profiles
- `ls` — catalog via the **GetTables RPC** (portable across every server we tested;
  see the [dialect matrix](https://sparrowflight.io/cli))
- `sql` — `CommandStatementQuery` → `GetFlightInfo` → `DoGet`, streamed
- Output contract: aligned table on a TTY, **raw Arrow IPC on stdout in a pipe**

Validated against GizmoSQL (DuckDB engine) from Linux and Windows.
Next (M1): `-o file.{arrow,parquet,csv,json}`, InfluxDB 3 bearer+header auth, Dremio quirks.

## Build

```
go build -o sparrow .        # needs Go >= 1.25 (arrow-go v18.6)
GOOS=windows go build -o sparrow.exe .
```
