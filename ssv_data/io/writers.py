"""Delta writers — the idempotent overwrite pattern shared by every gold/silver table."""


def write_delta(ctx, df, table: str, partition_col: str = None,
                replace_where: str = None, mode: str = "overwrite",
                overwrite_schema: bool = False):
    """Idempotent partitioned overwrite.

    - First run (table absent): plain partitioned overwrite -> creates the table.
    - Later runs with replace_where: atomic swap of just the affected slice.
    - overwrite_schema=True: allow the overwrite to CHANGE the table schema
      (Delta otherwise raises DELTA_0007). Needed by full-state tables that evolve,
      e.g. the SCD1 -> SCD2 dim migration adding valid_from/valid_to/is_current.

    This is the Fabric equivalent of the original ClickHouse pattern
        ALTER TABLE ... DELETE WHERE report_date = D ;  INSERT ...
    but atomic and without a heavy mutation.
    """
    from pyspark.sql import functions as F
    from pyspark.sql.types import NullType

    # Delta cannot store VOID/NullType columns. These appear when a source field is
    # null in every row of the window (very common from schemaless Mongo, e.g. an
    # all-null deliveryOrderNo). Cast them to string so the table stays queryable.
    void_cols = [f.name for f in df.schema.fields if isinstance(f.dataType, NullType)]
    for c in void_cols:
        df = df.withColumn(c, F.col(c).cast("string"))
    if void_cols and ctx.logger:
        ctx.logger.info(f"{table}: cast void columns to string -> {void_cols}")

    w = df.write.format(getattr(ctx, "table_format", "delta")).mode(mode)
    if overwrite_schema and mode == "overwrite":
        w = w.option("overwriteSchema", "true")
    if partition_col:
        w = w.partitionBy(partition_col)
    if getattr(ctx, "table_format", "delta") == "delta" and replace_where and ctx.spark.catalog.tableExists(table):
        w = w.option("replaceWhere", replace_where)
    w.saveAsTable(table)
    if ctx.logger:
        ctx.logger.info(f"wrote {table} (partition={partition_col}, replaceWhere={replace_where})")