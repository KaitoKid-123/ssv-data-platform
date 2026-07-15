from pyspark.sql import functions as F

def pivot_status_times(df, key: str, status_col: str, time_col: str, statuses: list):
    """Pivot an event/audit table to one max(time) column per status code.
 
    Returns: key + one timestamp column per status (named by the raw status value).
    Missing statuses are added as null so downstream selects never KeyError.
    Replaces the pandas pivot_table in export_delivery_status_info.
    """
    p = df.groupBy(key).pivot(status_col, statuses).agg(F.max(time_col))
    for s in statuses:
        if str(s) not in p.columns:
            p = p.withColumn(str(s), F.lit(None).cast("timestamp"))
    return p

def range_join_effective(fact, dim, keys, ts_col: str, eff_col: str, next_col: str,
                         broadcast: bool = True):
    """Left-join an effective-dated dimension and keep rows where
    ts in [eff, next) OR no dim row matched.
 
    Mirrors the original purchase-price behaviour exactly:
      - matched & in-range  -> kept with dim attrs
      - matched & out-of-range -> DROPPED (eff is non-null, predicate false)
      - no match -> kept, dim attrs null
    """
    d = F.broadcast(dim) if broadcast else dim
    out = fact.join(d, keys, "left")
    return out.where(
        ((F.col(eff_col) <= F.col(ts_col)) & (F.col(ts_col) < F.col(next_col)))
        | F.col(eff_col).isNull()
    )

def coalesce_sources(df, target: str, sources: list):
    """target = first non-null among `sources` (the fillna-priority idiom).
 
    Order matters — pass sources in priority order. Replaces the long
    df['x'] = df['a'].fillna(df['b']).fillna(...) chains in merge_all_data.
    """
    return df.withColumn(target, F.coalesce(*[F.col(c) for c in sources]))
 
def add_audit_columns(df, by: str = "default"):
    """Append created_at / updated_at / created_by / updated_by."""
    return (df
            .withColumn("created_at", F.current_timestamp())
            .withColumn("updated_at", F.current_timestamp())
            .withColumn("created_by", F.lit(by))
            .withColumn("updated_by", F.lit(by)))