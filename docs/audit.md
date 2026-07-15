# `sparrow audit` — the security-surface probe

A Flight SQL client sends arbitrary SQL, and a DuckDB-backed server runs it
with DuckDB's full default powers — which reach well past querying:

- read arbitrary files off the server host (`read_text`, `read_csv`, …)
- enumerate the filesystem (`glob`)
- write files (`COPY … TO`)
- open outbound connections — **SSRF** (`read_csv('http://…')`, `httpfs`)
- change server-wide configuration (raise `memory_limit` to OOM the node,
  or re-enable any of the above)

On a server that takes public or shared credentials — like a demo endpoint —
"authenticated" is not a barrier. `sparrow audit` probes each capability with
the **least-harmful** SQL that still proves it, and reports what the server
permits.

```sh
sparrow audit                 # audit the default profile
sparrow audit -s prod         # a named server
sparrow audit -o json         # machine-readable, for CI
```

**It is a defender's tool.** Every probe is benign — it reads
`/etc/hostname`, lists `/`, writes `/dev/null`, connects to a dead loopback
port, and flips an inert setting. It verifies hardening; it does not exploit,
pivot, or read sensitive data. Run it only against a server you operate or
are explicitly authorized to test.

## The probes

| probe | benign SQL | what "exposed" means |
|---|---|---|
| `file-read` | `read_text('/etc/hostname')` | client SQL can read host files |
| `dir-list` | `glob('/*')` | client SQL can enumerate the filesystem |
| `file-write` | `COPY (SELECT 1) TO '/dev/null'` | client SQL can write host files |
| `net-fetch` | `read_csv('http://127.0.0.1:1/…')` | server opens outbound connections (SSRF) |
| `ext-load` | `LOAD spatial` | client SQL can load extensions — arbitrary native code |
| `config-write` | `SET enable_progress_bar=false` | client SQL can change server config |

Each verdict is one of: **✗ exposed** (permitted — a real finding),
**✓ blocked** (the server refused by policy), **· n/a** (the primitive
isn't reachable — hardened, or a non-DuckDB engine), **⚠ unknown** (the
error was ambiguous). Exit **1** if anything is exposed, so a deploy can
gate on it.

## Real captures (2026-07-15)

The same audit, two servers. A hardened node:

```
sparrow audit — grpc+tls://flight.sparrowflight.io:443 (profile: demo)

 ✓ file-read     blocked by the server
 ✓ dir-list      blocked by the server
 ✓ file-write    blocked by the server
 ✓ net-fetch     blocked by the server
 ✓ ext-load      blocked by the server
 ✓ config-write  blocked by the server

clean — no exposed surface found                      → exit 0
```

A stock GizmoSQL server (DuckDB, default config):

```
 ✗ file-read     client SQL can read arbitrary files off the server host
               hint: DuckDB: SET enable_external_access=false (+ allowed_directories, lock_configuration)
 ✗ dir-list      client SQL can enumerate the server's filesystem
 ✗ file-write    client SQL can write files on the server host
 ✗ net-fetch     client SQL can make the server open outbound connections (SSRF …)
 ✗ config-write  client SQL can change server-wide DuckDB config …

5 exposed — this server lets client SQL reach beyond querying   → exit 1
```

## Hardening a DuckDB-backed server

This follows DuckDB's own [Securing DuckDB](https://duckdb.org/docs/stable/operations_manual/securing_duckdb/overview)
manual. Run these once at startup, **after** any legitimate extension/config
setup (load the extensions you need *first* — already-loaded ones keep
working), on the connection that serves client SQL, in this order:

```sql
-- 1. file + network access
SET allowed_directories = ['/path/to/data', '/path/to/spill'];
SET enable_external_access = false;
-- 2. extension lockdown (no arbitrary native code, no httpfs autoload)
SET autoinstall_known_extensions = false;
SET autoload_known_extensions   = false;
SET allow_community_extensions  = false;
-- 3. resource caps (DoS) — memory_limit + a spill cap
SET memory_limit = '2GB';
SET max_temp_directory_size = '10GB';
-- 4. LAST — freeze all of the above so client SQL can't undo it
SET lock_configuration = true;
```

`allowed_directories` carves back exactly the paths the server needs (data,
any spill directory for large aggregates). After `lock_configuration`, a
re-audit should read all **✓ blocked**.

Two things DuckDB's manual stresses that this SQL can't do for you:

- **OS/container sandboxing is the foundation** — these settings are
  defense-in-depth, not a substitute for running the server in a locked-down
  container with minimal capabilities and network egress controls.
- **Don't run as root.** `audit` can't see the process UID over Flight, so
  check it yourself (`docker exec <c> id`); DuckDB's guidance is plain —
  "there is no good reason to run DuckDB as root."

## Caveats

- The probes are DuckDB-flavored. Against a non-DuckDB engine (DataFusion,
  etc.) they read **· n/a** — an honest "not reachable", *not* a clean bill
  of health for that engine's own surface.
- On an **unhardened** DuckDB server, `net-fetch` and `ext-load` make the
  server autoload/install an extension (a one-time download) — expected, and
  itself part of the finding (the server will fetch + run native extensions
  on client demand).
