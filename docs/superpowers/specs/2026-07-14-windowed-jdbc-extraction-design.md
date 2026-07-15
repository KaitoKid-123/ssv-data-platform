# Windowed JDBC extraction for PostgreSQL transactional tables

**Date:** 2026-07-14 · **Status:** approved · **Scope:** dev/local JDBC path only (prod OneLake shortcut unchanged)

## Problem

`ingest_postgres` full-extracts all 12 PG tables (`SELECT * FROM public.{t}`) every run.
The transactional tables (`delivery_orders`, `delivery_order_audits`, `delivery_order_details`,
`sale_transactions`, `point_histories`) grow daily, so the dev JDBC extract gets slower over time.
`RunWindow` already computes the run's UTC window but the readers never use it.

## Design

Mechanism in the library, policy in the pipeline (functional core / config over code).

### 1. `ssv_data/runtime/window.py`

Add `cover_hi: datetime` to `RunWindow` = `utc_hi + cover_days` — symmetric with the existing
`cover_lo`. PG extraction windows on `[cover_lo, cover_hi)`; the Mongo bill window stays strict
`[utc_lo, utc_hi)` (silver `sale_line` writes with `replaceWhere(report_date)`, so bills must
match the run day exactly).

### 2. `ssv_data/io/readers.py`

Two pure SQL builders (unit-testable without Spark) + two thin `Readers` methods that delegate
to the existing `jdbc()`:

- `windowed_select(table, ts_col, window)` →
  `SELECT * FROM {table} WHERE {ts_col} >= TIMESTAMP '<cover_lo>' AND {ts_col} < TIMESTAMP '<cover_hi>'`
- `parent_windowed_select(table, parent_table, key, parent_ts_col, window)` →
  `SELECT c.* FROM {table} c WHERE EXISTS (SELECT 1 FROM {parent_table} p WHERE p.{key} = c.{key} AND p.{parent_ts_col} in window)`
- `Readers.jdbc_windowed(...)`, `Readers.jdbc_parent_windowed(...)`

The whole predicate is pushed down to PG (Spark JDBC `query` option), so transferred bytes are
proportional to a day's volume, not table size.

### 3. `sample_file/bronze.py.ipynb` — per-table policy

| Group | Tables | Strategy |
|---|---|---|
| `PG_WINDOWED` | `delivery_orders`, `sale_transactions`, `point_histories` | window on own `created_at` |
| `PG_ORDER_CHILDREN` | `delivery_order_audits`, `delivery_order_details` | semi-join on windowed `delivery_orders` (audit events can land after midnight; details has no timestamp) |
| `PG_FULL` + `DIM_TABLES` | `users`, `oms_product`, `oms_store`, `mapping_payment_method`, 3 dims | full extract (master data, no timestamps) |

`ingest_postgres(ctx, full_load=False)` keeps a full-load escape hatch for repair backfills.

## Correctness invariant

Gold output for the run day must equal the full-load result. Holds because:

- `created_at` is write-once → late **UPDATEs** (status, rating, comment) are always re-read;
  only rows **INSERTed** with `created_at` outside `[cover_lo, cover_hi)` can be missed.
- Silver `delivery`/`transaction`/`point_history` are full overwrites (no `replaceWhere`) →
  out-of-day rows in bronze are harmless.
- Gold filters `report_date == running_date` last → cover-window extras drop out, same as full load.
- Canceled path (`report_date = to_date(dom_created_at)`, UTC date-part) needs orders with
  `created_at ∈ [D 00:00, D+1 00:00 UTC)`; `[cover_lo, cover_hi)` ⊇ that range. (`cover_hi` is
  required — `[cover_lo, utc_hi)` alone would lose orders created `[D 17:00, D+1 00:00 UTC)`.)

## Accepted limitations (user-confirmed 2026-07-14)

1. Rows inserted with `created_at` > 1 day away from the bill day are missed on backfill
   (e.g. pre-orders placed > 24h ahead). Confirmed: 7Now does not take pre-orders > 24h.
   Mitigation: `cover_days` parameter of `get_run_window`; `full_load=True` repair path.
2. `point_histories` dedup (`rn=1` by `is_captured desc`) can pick a different row when a
   transaction's point rows span the cover boundary — affects `capture_method` only, not money.

No financial columns and no DQ-gated columns are affected; all enrichment joins are left joins.

## Ops note (out of scope)

Real PG should index `delivery_orders(created_at)` and `order_id` on both child tables for the
EXISTS pushdown.

## Tests

- `tests/test_window.py` — exact `utc_lo/utc_hi/cover_lo/cover_hi` for 2025-11-17; custom `cover_days`.
- `tests/test_readers_windowed.py` — exact SQL from both builders; `Readers` wiring via fake
  SparkSession (no pyspark needed).

Version bump: `0.1.1 → 0.1.2`.
