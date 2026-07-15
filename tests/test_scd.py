"""SCD2 write side (scd2_initial / scd2_apply) + read side (as_of_join) — local Spark.

Cases mirror the design walkthrough in docs/superpowers/specs/2026-07-14-scd2-dims-design.md.
"""
import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from ssv_data.transforms.scd import as_of_join, scd2_apply, scd2_initial


@pytest.fixture(scope="module")
def spark():
    s = (SparkSession.builder.master("local[2]").appName("scd2-tests")
         .config("spark.sql.shuffle.partitions", "2")
         .config("spark.ui.enabled", "false")
         .getOrCreate())
    yield s
    s.stop()


DIM_SCHEMA = "store_id string, tier string, sale_area_id int"
KEYS = ["store_id"]


def _incoming(spark, rows):
    return spark.createDataFrame(rows, DIM_SCHEMA)


def _state(df):
    """Comparable snapshot: (key, tier, area, valid_from-date, valid_to-date, is_current)."""
    rows = [(r.store_id, r.tier, r.sale_area_id,
             str(r.valid_from)[:10], str(r.valid_to)[:10], r.is_current)
            for r in df.collect()]
    return sorted(rows, key=lambda t: tuple(str(x) for x in t))  # None-safe ordering


# ---- Case A: bootstrap ----
def test_initial_load_opens_every_key_at_the_epoch(spark):
    st = scd2_initial(_incoming(spark, [("S1", "T1", 1), ("S2", "T1", 2)]), KEYS)
    assert _state(st) == [
        ("S1", "T1", 1, "1970-01-01", "9999-12-31", True),
        ("S2", "T1", 2, "1970-01-01", "9999-12-31", True),
    ]


# ---- Case B/D: idempotency ----
def test_no_change_is_a_noop(spark):
    cur = scd2_initial(_incoming(spark, [("S1", "T1", 1)]), KEYS)
    nxt = scd2_apply(cur, _incoming(spark, [("S1", "T1", 1)]), KEYS, "2026-07-10")
    assert _state(nxt) == _state(cur)


# ---- Case C: change closes the old version and opens a new one ----
def test_change_closes_and_opens_versions(spark):
    cur = scd2_initial(_incoming(spark, [("S1", "T1", 1)]), KEYS)
    nxt = scd2_apply(cur, _incoming(spark, [("S1", "T2", 1)]), KEYS, "2026-07-10")
    assert _state(nxt) == [
        ("S1", "T1", 1, "1970-01-01", "2026-07-10", False),
        ("S1", "T2", 1, "2026-07-10", "9999-12-31", True),
    ]


# ---- Case C guard: an old-day backfill must not rewrite history ----
def test_forward_only_guard_skips_older_effective_dates(spark):
    cur = scd2_initial(_incoming(spark, [("S1", "T1", 1)]), KEYS)
    cur = scd2_apply(cur, _incoming(spark, [("S1", "T2", 1)]), KEYS, "2026-07-10")
    nxt = scd2_apply(cur, _incoming(spark, [("S1", "T9", 9)]), KEYS, "2026-07-01")
    assert _state(nxt) == _state(cur)  # unchanged — T9 is today's state, not July 1st's


# ---- Case E: same-day re-run with another change replaces in place ----
def test_same_day_rechange_replaces_without_zero_length_version(spark):
    cur = scd2_initial(_incoming(spark, [("S1", "T1", 1)]), KEYS)
    cur = scd2_apply(cur, _incoming(spark, [("S1", "T2", 1)]), KEYS, "2026-07-10")
    nxt = scd2_apply(cur, _incoming(spark, [("S1", "T3", 1)]), KEYS, "2026-07-10")
    assert _state(nxt) == [
        ("S1", "T1", 1, "1970-01-01", "2026-07-10", False),
        ("S1", "T3", 1, "2026-07-10", "9999-12-31", True),  # T2 gone — replaced in place
    ]


# ---- new key / gone key (Case G) ----
def test_new_key_opens_at_effective_date(spark):
    cur = scd2_initial(_incoming(spark, [("S1", "T1", 1)]), KEYS)
    nxt = scd2_apply(cur, _incoming(spark, [("S1", "T1", 1), ("S2", "T1", 2)]), KEYS, "2026-07-10")
    assert ("S2", "T1", 2, "2026-07-10", "9999-12-31", True) in _state(nxt)


def test_gone_key_is_soft_closed(spark):
    cur = scd2_initial(_incoming(spark, [("S1", "T1", 1), ("S2", "T1", 2)]), KEYS)
    nxt = scd2_apply(cur, _incoming(spark, [("S1", "T1", 1)]), KEYS, "2026-07-10")
    assert ("S2", "T1", 2, "1970-01-01", "2026-07-10", False) in _state(nxt)


def test_key_created_and_gone_same_day_vanishes(spark):
    cur = scd2_initial(_incoming(spark, [("S1", "T1", 1)]), KEYS)
    cur = scd2_apply(cur, _incoming(spark, [("S1", "T1", 1), ("S2", "T1", 2)]), KEYS, "2026-07-10")
    nxt = scd2_apply(cur, _incoming(spark, [("S1", "T1", 1)]), KEYS, "2026-07-10")
    assert [r for r in _state(nxt) if r[0] == "S2"] == []  # no zero-length ghost


# ---- null-safe change detection ----
def test_null_to_null_is_not_a_change_but_null_to_value_is(spark):
    cur = scd2_initial(_incoming(spark, [("S1", None, 1)]), KEYS)
    same = scd2_apply(cur, _incoming(spark, [("S1", None, 1)]), KEYS, "2026-07-10")
    assert _state(same) == _state(cur)
    nxt = scd2_apply(cur, _incoming(spark, [("S1", "T1", 1)]), KEYS, "2026-07-10")
    assert ("S1", "T1", 1, "2026-07-10", "9999-12-31", True) in _state(nxt)


# ---- Case H/I: as_of_join ----
@pytest.fixture()
def dim_versions(spark):
    cur = scd2_initial(_incoming(spark, [("S1", "T1", 1)]), KEYS)
    cur = scd2_apply(cur, _incoming(spark, [("S1", "T1", 2)]), KEYS, "2026-07-10")   # area 1 -> 2
    return scd2_apply(cur, _incoming(spark, [("S1", "T1", 2), ("S2", "T1", 7)]), KEYS, "2026-07-12")


def _facts(spark, rows):
    return spark.createDataFrame(rows, "txn string, store_id string, ts timestamp")


def test_as_of_join_picks_the_version_covering_ts(spark, dim_versions):
    from datetime import datetime
    facts = _facts(spark, [("t1", "S1", datetime(2026, 7, 9, 23, 59, 59)),
                           ("t2", "S1", datetime(2026, 7, 10, 0, 0, 0))])  # boundary: half-open
    out = {r.txn: r.sale_area_id
           for r in as_of_join(facts, dim_versions, KEYS, "ts").collect()}
    assert out == {"t1": 1, "t2": 2}


def test_as_of_join_keeps_fact_rows_with_no_covering_version(spark, dim_versions):
    from datetime import datetime
    facts = _facts(spark, [("t3", "S2", datetime(2026, 7, 11, 12, 0, 0)),   # before S2's first version
                           ("t4", "S9", datetime(2026, 7, 11, 12, 0, 0))])  # unknown key
    rows = as_of_join(facts, dim_versions, KEYS, "ts").collect()
    assert len(rows) == 2                                # rows preserved, never dropped
    assert all(r.sale_area_id is None for r in rows)     # attrs null


def test_as_of_join_drops_scd2_columns_from_output(spark, dim_versions):
    from datetime import datetime
    facts = _facts(spark, [("t1", "S1", datetime(2026, 7, 11, 0, 0, 0))])
    cols = as_of_join(facts, dim_versions, KEYS, "ts").columns
    assert "valid_from" not in cols and "valid_to" not in cols and "is_current" not in cols
