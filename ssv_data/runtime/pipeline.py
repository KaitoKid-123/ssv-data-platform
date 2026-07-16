"""MedallionPipeline — the thin OOP shell every pipeline subclasses.

Template Method: the fixed orchestration (run order, logging, error handling,
per-day backfill loop) lives here once; subclasses fill in the three domain
steps. Keep this class about *coordination only* — all data logic stays in
pure functions under transforms/ and in the pipeline's own modules.
"""
from abc import ABC, abstractmethod
from datetime import datetime, timedelta

from ssv_data.runtime.context import PipelineContext
from ssv_data.runtime.logging import get_logger
from ssv_data.runtime.window import get_run_window


class MedallionPipeline(ABC):
    name: str = "unnamed"

    def __init__(self, spark=None, secret=None, schema_enabled: bool = True, table_format: str = "delta"):
        self.spark = spark or self._default_spark()
        self.secret = secret
        self.schema_enabled = schema_enabled
        self.table_format = table_format
        self.logger = get_logger(f"ssv_data.{self.name}")

    @staticmethod
    def _default_spark():
        from pyspark.sql import SparkSession
        return SparkSession.builder.getOrCreate()

    def context(self, running_date: str) -> PipelineContext:
        kwargs = dict(spark=self.spark, window=get_run_window(running_date),
                      schema_enabled=self.schema_enabled, table_format=self.table_format,
                      logger=self.logger)
        if self.secret is not None:
            kwargs["secret"] = self.secret
        return PipelineContext(**kwargs)

    # ---- subclasses implement these three ----
    @abstractmethod
    def ingest(self, ctx: PipelineContext) -> None: ...

    @abstractmethod
    def build_silver(self, ctx: PipelineContext) -> None: ...

    @abstractmethod
    def build_gold(self, ctx: PipelineContext) -> None: ...

    # ---- fixed orchestration (do not override) ----
    def run(self, running_date: str, with_ingest: bool = True) -> None:
        ctx = self.context(running_date)
        self.logger.info(f"[{self.name}] start {running_date}")
        steps = ([("ingest", self.ingest)] if with_ingest else []) \
            + [("silver", self.build_silver), ("gold", self.build_gold)]
        records = []
        for step, fn in steps:
            started = datetime.utcnow()
            try:
                fn(ctx)
                records.append((step, "Succeeded", started, datetime.utcnow(), None))
            except Exception as e:
                records.append((step, "Failed", started, datetime.utcnow(), str(e)[:2000]))
                self._write_run_log(ctx, running_date, records)
                self.logger.exception(f"[{self.name}] FAILED {running_date} at {step}")
                raise
        self._write_run_log(ctx, running_date, records)
        self.logger.info(f"[{self.name}] done {running_date}")

    def _write_run_log(self, ctx, running_date: str, records) -> None:
        """Append step timings/status to ops.run_log (ops_run_log when schemas are off).

        Observability only — best-effort by design: a logging failure must never fail
        the run, so any exception here is swallowed into a warning."""
        try:
            from pyspark.sql import types as T
            schema = T.StructType([
                T.StructField("pipeline", T.StringType()),
                T.StructField("run_date", T.StringType()),
                T.StructField("step", T.StringType()),
                T.StructField("status", T.StringType()),
                T.StructField("started_at", T.TimestampType()),
                T.StructField("ended_at", T.TimestampType()),
                T.StructField("duration_s", T.DoubleType()),
                T.StructField("error", T.StringType()),
            ])
            rows = [(self.name, running_date, step, status, a, b,
                     (b - a).total_seconds(), err)
                    for step, status, a, b, err in records]
            if getattr(ctx, "schema_enabled", True):
                ctx.spark.sql("CREATE SCHEMA IF NOT EXISTS ops")
            (ctx.spark.createDataFrame(rows, schema)
                .write.format(getattr(ctx, "table_format", "delta"))
                .mode("append").saveAsTable(ctx.table("ops", "run_log")))
        except Exception as e:  # noqa: BLE001 — observability must not break the pipeline
            self.logger.warning(f"[{self.name}] run_log write skipped: {e}")

    def backfill(self, start_date: str, end_date: str, with_ingest: bool = True) -> None:
        """Inclusive start, exclusive end. Reruns run() per day; idempotent via replaceWhere."""
        d = datetime.strptime(start_date, "%Y-%m-%d")
        end = datetime.strptime(end_date, "%Y-%m-%d")
        while d < end:
            self.run(d.strftime("%Y-%m-%d"), with_ingest=with_ingest)
            d += timedelta(days=1)