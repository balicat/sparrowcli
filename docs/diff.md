# `sparrow diff` — the drift gate

Compare **one table on two servers**: primary vs replica, prod vs staging,
lake vs serving snapshot. Everything runs as conservative server-side
aggregates — the table itself is never downloaded from either side.

```sh
sparrow diff series_data --against gizmo --time period
sparrow diff trades --against grpc+tls://replica.example.com:443
sparrow diff series_data --against gizmo -o json     # machine-readable
```

- **A side** = the normal connection (`-s profile`, or the default profile).
- **B side** = `--against`: a saved profile name, or an **anonymous** URI.
  For an authenticated ad-hoc B, save a profile first
  (`sparrow connect <uri> --basic user:pass --name replica`).

## What it compares

| check | how |
|---|---|
| schema | `LIMIT 0` on both sides — added / dropped / retyped columns |
| rows | `COUNT(*)` |
| time (`--time col`) | `MIN`/`MAX` bounds of the column |
| numeric fingerprint | `COUNT` + `AVG` of up to 4 shared numeric columns |

Numeric comparison uses a relative tolerance (1e-9), so engine-specific
float summation order doesn't read as drift.

## Exit codes are the point

Identical → **exit 0**. Any difference → **exit 1**. A cron line becomes a
replication monitor:

```sh
sparrow diff series_data --against replica --time period \
  || notify "replica has drifted"
```

## Real captures

Two *different server implementations* serving the same snapshot, proven
identical across 136M rows — four aggregates, nothing downloaded:

```
sparrow diff series_data
A: demo       grpc+tls://flight.sparrowflight.io:443
B: gizmo2     grpc+tls://flight2.sparrowflight.io:443

 ✓ schema       3 columns, identical
 ✓ rows         136,052,269
 ✓ time         1859 → 2027Q4
 ✓ Σ value      count 136,052,269 · avg 1.113048882512338e+07

4 checks — identical
```

And drift, caught:

```
sparrow diff drift_t
A: g1         grpc+tls://localhost:31337
B: g2         grpc://localhost:31339

 ✓ schema       2 columns, identical
 ✗ rows         A 3 · B 2
 ✗ Σ id         A count 3 · avg 2 · B count 2 · avg 1.5
 ✗ Σ v          A count 3 · avg 20.5 · B count 2 · avg 55.2

1 same · 3 differ — drift detected
```
