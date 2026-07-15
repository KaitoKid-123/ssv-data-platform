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
        try:
            if with_ingest:
                self.ingest(ctx)
            self.build_silver(ctx)
            self.build_gold(ctx)
            self.logger.info(f"[{self.name}] done {running_date}")
        except Exception:
            self.logger.exception(f"[{self.name}] FAILED {running_date}")
            raise

    def backfill(self, start_date: str, end_date: str, with_ingest: bool = True) -> None:
        """Inclusive start, exclusive end. Reruns run() per day; idempotent via replaceWhere."""
        d = datetime.strptime(start_date, "%Y-%m-%d")
        end = datetime.strptime(end_date, "%Y-%m-%d")
        while d < end:
            self.run(d.strftime("%Y-%m-%d"), with_ingest=with_ingest)
            d += timedelta(days=1)