"""Source readers, bound to a PipelineContext.

PRODUCTION ingestion strategy (per source):
  - PostgreSQL (7reward / som / oms, self-hosted + Airbyte CDC):
        Airbyte lands Parquet/Delta in ADLS/S3 -> OneLake shortcut.
        Read it as a normal table:  Readers(ctx).table("bronze", "delivery_orders").
        => zero ingest code; .table() IS the production path. This now covers the 3 dims
           too (product_uom_upc, dict_stores, purchase_price_timeline), moved off ClickHouse.
  - MongoDB (report / delivery_order):
        Copy activity (Mongo connector) OR Readers(ctx).mongo(...) for the daily window.
  - Custom REST (DLM): plain `requests` in the pipeline (no reader — see eod_sale/bronze.py).
  - jdbc(): dev/local extract of Postgres (bronze uses it when a `pg-jdbc` secret is set),
        or any ad-hoc JDBC source. Transactional tables should go through
        jdbc_windowed() / jdbc_parent_windowed() so only the run's cover window
        [ctx.window.cover_lo, ctx.window.cover_hi) is extracted — the predicate is
        pushed down to Postgres via the JDBC `query` option.
"""


def _ts_literal(dt) -> str:
    """Render a datetime as a standard SQL timestamp literal (Postgres-compatible)."""
    return f"TIMESTAMP '{dt.strftime('%Y-%m-%d %H:%M:%S')}'"


def windowed_select(table: str, ts_col: str, window) -> str:
    """SELECT rows whose ts_col falls in the run's cover window [cover_lo, cover_hi).

    ts_col must be write-once (e.g. created_at): rows are re-read with their CURRENT
    values, so late UPDATEs are still captured on backfill — only rows INSERTed with
    an out-of-cover timestamp are missed.
    """
    return (f"SELECT * FROM {table} "
            f"WHERE {ts_col} >= {_ts_literal(window.cover_lo)} "
            f"AND {ts_col} < {_ts_literal(window.cover_hi)}")


def parent_windowed_select(table: str, parent_table: str, key: str,
                           parent_ts_col: str, window) -> str:
    """SELECT child rows whose PARENT falls in the cover window (EXISTS semi-join).

    For child tables with no usable timestamp of their own (delivery_order_details)
    or whose events can land after the window (delivery_order_audits: an order created
    23:30 completes 00:30 next day — its audit row must still be extracted).
    """
    return (f"SELECT c.* FROM {table} c WHERE EXISTS ("
            f"SELECT 1 FROM {parent_table} p "
            f"WHERE p.{key} = c.{key} "
            f"AND p.{parent_ts_col} >= {_ts_literal(window.cover_lo)} "
            f"AND p.{parent_ts_col} < {_ts_literal(window.cover_hi)})")


class Readers:
    def __init__(self, ctx):
        self.ctx = ctx
        self.spark = ctx.spark

    def table(self, layer: str, name: str):
        """Read a managed/shortcut table by name — the production path for PG."""
        return self.spark.read.table(self.ctx.table(layer, name))

    def mongo(self, database: str, collection: str, pipeline: str = None):
        r = (self.spark.read.format("mongodb")
             .option("connection.uri", self.ctx.secret("mongo-conn"))
             .option("database", database)
             .option("collection", collection))
        if pipeline:
            r = r.option("aggregation.pipeline", pipeline)
        return r.load()

    def jdbc(self, url_secret: str, query: str, fetchsize: int = 10000):
        return (self.spark.read.format("jdbc")
                .option("url", self.ctx.secret(url_secret))
                .option("query", query)
                .option("fetchsize", str(fetchsize))
                .load())

    def jdbc_windowed(self, url_secret: str, table: str, ts_col: str, fetchsize: int = 10000):
        """Incremental JDBC extract: rows with ts_col in the run's cover window."""
        return self.jdbc(url_secret, windowed_select(table, ts_col, self.ctx.window), fetchsize)

    def jdbc_parent_windowed(self, url_secret: str, table: str, parent_table: str,
                             key: str, parent_ts_col: str, fetchsize: int = 10000):
        """Incremental JDBC extract of a child table, semi-joined on its windowed parent."""
        return self.jdbc(url_secret,
                         parent_windowed_select(table, parent_table, key, parent_ts_col,
                                                self.ctx.window),
                         fetchsize)