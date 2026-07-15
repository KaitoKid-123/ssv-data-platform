"""SCD Type 2 for conformed dims — the WRITE side of slow-changing dimensions.

The READ side has always existed here: purchase_price_timeline is effective-dated and
gold joins it with range_join_effective(). These helpers let silver dims accumulate the
same shape (valid_from / valid_to / is_current).

Dims are small (100s-1000s of rows), so scd2_apply returns the FULL next state and the
caller overwrites the table — no MERGE machinery. Timestamps are naive VN-local, like
transaction_time (documentDate + 7h), so version boundaries align with the VN business day.
"""
from datetime import datetime
from functools import reduce
from operator import and_

from pyspark.sql import functions as F

SCD2_COLS = ["valid_from", "valid_to", "is_current"]
START_OF_TIME = "1970-01-01"   # epoch sentinel: pre-adoption backfills still match
END_OF_TIME = "9999-12-31"


def has_scd2_shape(df) -> bool:
    return all(c in df.columns for c in SCD2_COLS)


def _row_hash(cols):
    """Null-safe change-detection hash over the tracked columns."""
    return F.sha2(F.concat_ws(
        "\x1f", *[F.coalesce(F.col(c).cast("string"), F.lit("\x00")) for c in cols]), 256)


def scd2_initial(incoming, keys):
    """Bootstrap: every incoming row becomes the open version, valid from the epoch."""
    return (incoming
            .withColumn("valid_from", F.to_timestamp(F.lit(START_OF_TIME)))
            .withColumn("valid_to", F.to_timestamp(F.lit(END_OF_TIME)))
            .withColumn("is_current", F.lit(True)))


def scd2_apply(current, incoming, keys, effective_date: str, tracked=None):
    """Next full state of an SCD2 dim, given today's source snapshot.

    - unchanged key            -> kept as-is
    - changed key              -> close the open version at effective_date + open a new one
    - changed again same day   -> the same-day version is replaced in place (no 0-length rows)
    - new key                  -> open version at effective_date
    - key gone from the source -> soft close (historical joins still match)
    - effective_date OLDER than the newest version -> return current unchanged: an old-day
      backfill extracts TODAY's master state; recording it under an old date would corrupt
      history. (This check collect()s one max() over a small dim — accepted.)
    """
    biz_cols = [c for c in incoming.columns if c not in SCD2_COLS]
    tracked = tracked or [c for c in biz_cols if c not in keys]
    eff = F.to_timestamp(F.lit(effective_date))

    # forward-only guard
    max_vf = current.agg(F.max("valid_from").alias("m")).collect()[0]["m"]
    if max_vf is not None and datetime.strptime(effective_date, "%Y-%m-%d") < max_vf:
        return current

    closed_hist = current.where(~F.col("is_current"))
    open_cur = current.where(F.col("is_current"))
    inc = incoming.select(*biz_cols)

    cmp = (open_cur.withColumn("_h", _row_hash(tracked))
           .select(*keys, F.col("_h").alias("_cur_h"), F.col("valid_from").alias("_vf"))
           .join(inc.withColumn("_h", _row_hash(tracked))
                 .select(*keys, F.col("_h").alias("_inc_h")),
                 keys, "full"))
    unchanged = cmp.where(F.col("_cur_h") == F.col("_inc_h")).select(*keys)
    changed = cmp.where(F.col("_cur_h").isNotNull() & F.col("_inc_h").isNotNull()
                        & (F.col("_cur_h") != F.col("_inc_h")))
    added = cmp.where(F.col("_cur_h").isNull()).select(*keys)
    gone = cmp.where(F.col("_inc_h").isNull())

    kept = open_cur.join(unchanged, keys, "left_semi")
    # versions opened BEFORE today get closed; a version opened TODAY is simply dropped
    # (replaced by the new open version / removed if its key vanished the same day)
    close = lambda marker: (open_cur
                            .join(marker.where(F.col("_vf") < eff).select(*keys), keys, "left_semi")
                            .withColumn("valid_to", eff)
                            .withColumn("is_current", F.lit(False)))
    opened = (inc.join(changed.select(*keys).unionByName(added), keys, "left_semi")
              .withColumn("valid_from", eff)
              .withColumn("valid_to", F.to_timestamp(F.lit(END_OF_TIME)))
              .withColumn("is_current", F.lit(True)))

    return (closed_hist
            .unionByName(kept)
            .unionByName(close(changed))
            .unionByName(close(gone))
            .unionByName(opened))


def as_of_join(left, dim, keys, ts_col: str, broadcast: bool = True):
    """SCD2 point-in-time LEFT join: pick the dim version covering left.ts_col.

    Unlike range_join_effective (the purchase-price contract, which DROPS matched
    out-of-range rows), every left row is preserved — dim attrs are null when no
    version covers ts (new key, gap after a soft delete, unknown key). Half-open
    [valid_from, valid_to). SCD2 columns are dropped from the output.
    """
    d = dim.drop("is_current")
    r = (F.broadcast(d) if broadcast else d).alias("_r")
    l = left.alias("_l")
    cond = [F.col(f"_l.{k}") == F.col(f"_r.{k}") for k in keys]
    cond.append(F.col("_r.valid_from") <= F.col(f"_l.{ts_col}"))
    cond.append(F.col(f"_l.{ts_col}") < F.col("_r.valid_to"))
    attr_cols = [c for c in d.columns if c not in keys and c not in ("valid_from", "valid_to")]
    return l.join(r, reduce(and_, cond), "left").select("_l.*", *[F.col(f"_r.{c}") for c in attr_cols])
