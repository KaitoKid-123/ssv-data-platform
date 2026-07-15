# SCD Type 2 for dim_store + dim_product

**Date:** 2026-07-14 · **Status:** approved · **Depends on:** windowed-jdbc-extraction (0.1.2)

## Problem

Silver dims are truncate-reloaded from the source's CURRENT state each run, and gold
denormalizes dim attributes into the fact at build time. When a store changes
`sale_area_id` (drives the purchase-price lookup) or a product changes category
(drives canceled-row attributes), **backfilling an old day silently picks up today's
values** — historical facts drift. The consumption side of SCD2 already exists and
runs in prod: `purchase_price_timeline` is effective-dated and gold joins it with
`range_join_effective`.

## Decisions (user-approved 2026-07-14)

1. **Scope:** `dim_store` + `dim_product` only (config-extensible).
2. **Shape:** convert `silver.dim_*` in place to SCD2 (`valid_from`/`valid_to`/`is_current`).
   Breaking for BI consumers: they must filter `is_current` for the current view.
3. **Gold:** as-of join on `transaction_time` (and `dom_created_at` for the canceled path).

## Design

### Mechanism — `ssv_data/transforms/scd.py` (new)

- `scd2_initial(incoming, keys)` — bootstrap: every row becomes the open version with
  `valid_from = 1970-01-01` (epoch sentinel so pre-adoption backfills still match),
  `valid_to = 9999-12-31`, `is_current = true`.
- `scd2_apply(current, incoming, keys, effective_date, tracked=None)` — returns the
  **full next state** (dims are small; plain overwrite, no MERGE):
  - unchanged key (null-safe hash over tracked cols) → kept as-is
  - changed key → close open version (`valid_to = effective_date`) + insert new open version
  - changed again same day (re-run) → open version with `valid_from == effective_date`
    is replaced in place — no zero-length versions
  - new key → open version at `effective_date`
  - key gone from source → soft close (historical joins still match)
  - **forward-only guard:** `effective_date < max(valid_from)` → return `current`
    unchanged (an old-day backfill extracts today's master state; writing it under an
    old effective date would corrupt history)
  - note: the guard does one tiny `.collect()` (max over a small dim) — accepted.
- `as_of_join(left, dim, keys, ts_col)` — SCD2 point-in-time LEFT join that **keeps
  every left row** (null attrs when no covering version) and drops the SCD2 columns
  from the output.

  > Deviation from the presented design (approved design said `range_join_effective`):
  > `range_join_effective` mirrors the purchase-price contract — a key that matches but
  > is out of range is **dropped**. For dims that contract would DELETE fact lines
  > (e.g. a transaction dated before a new store's first version). `as_of_join` uses the
  > range as a join condition instead of a post-filter, preserving fact rows. Same
  > half-open `[valid_from, valid_to)` semantics.

Timestamps are naive VN-local, consistent with `transaction_time` (documentDate + 7h)
and `report_date` — version boundaries align with the VN business day.

### Policy — silver notebook

```python
SCD2_DIMS = {"dim_product": ["product_id"], "dim_store": ["store_id"]}
```

`_write_scd2_dim(ctx, name, incoming, keys)`: existing table with SCD2 shape →
`scd2_apply`; missing table or legacy SCD1 shape → `scd2_initial` (auto-migration).
`localCheckpoint()` before overwriting the table that was read. Other dims unchanged.

### Gold

- `_merge`: `dim_store` leaves the equi-join chain → `as_of_join(fact, d_store,
  ["store_id"], "transaction_time")` **before** the price range join (sale_area_id is a
  price key; current order already satisfies this).
- `_canceled`: `dim_product` attrs via `as_of_join(..., ts_col="dom_created_at")`.

## Verified cases (design walkthrough)

A. legacy SCD1 table → auto-migrate via `scd2_initial`; pre-deploy backfills match ✅
B. no changes → hash-equal, state unchanged, idempotent ✅
C. tier change on D+1, then backfill D → guard skips write; as-of join returns the old
   version — **historically correct backfill** (the point of the feature) ✅
D. same-day re-run, same data → no-op ✅
E. same-day re-run after another source change → in-place replace, no zero-length version ✅
F. ascending multi-day backfill right after deploy → incoming == initial state → no junk versions ✅
G. product deactivated (drops out of the `status=1` filter) → soft close; earlier days
   still join, later days null (matches today's behavior) ✅
H. ts exactly at a version boundary → half-open `[from, to)`, consistent with
   `range_join_effective` prod semantics ✅
I. fact ts before a key's first version → row KEPT with null attrs (this is why
   `as_of_join` exists instead of `range_join_effective`) ✅

## Accepted limitations

1. History accumulates only from deployment; earlier history is unknowable from bronze
   (no audit log at the source).
2. `effective_date` = the run day that *detected* the change, not the true source-side
   change moment. Daily runs → day-accurate; a paused pipeline lumps changes onto the
   catch-up day.

## Post-deploy fix (0.1.4)

First Fabric run (2025-11-29) failed at `nb_transform` with `DELTA_0007` (schema
mismatch): the SCD1 → SCD2 migration overwrite adds 3 columns, but Delta rejects
schema changes on overwrite by default and `write_delta` never set `overwriteSchema`.
Local tests run parquet, which allows schema drift silently — the gap only shows on
real Delta. Fix: `write_delta(..., overwrite_schema=True)` param (0.1.4), used by
`_write_scd2_dim`; regression test in `tests/test_writers.py`.

## Tests

`tests/test_scd.py` on a local SparkSession (JDK21 + pyspark, per repo CI): cases A–I
plus null-safe compare (null→value is a change; null==null is not).

Version bump: `0.1.2 → 0.1.3`. Out of scope (YAGNI): `merge_delta` writer, watermark
store, SCD2 for other dims.
