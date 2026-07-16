"""MedallionPipeline writes step timings to the ops run log — local Spark, parquet mode."""
import pytest
from pyspark.sql import SparkSession

from ssv_data.runtime.pipeline import MedallionPipeline


@pytest.fixture(scope="module")
def spark(tmp_path_factory):
    wh = str(tmp_path_factory.mktemp("wh"))
    s = (SparkSession.builder.master("local[2]").appName("runlog-tests")
         .config("spark.sql.shuffle.partitions", "2")
         .config("spark.sql.warehouse.dir", wh)
         .config("spark.ui.enabled", "false")
         .getOrCreate())
    yield s
    s.stop()


class _Ok(MedallionPipeline):
    name = "ok_pipe"

    def ingest(self, ctx):
        pass

    def build_silver(self, ctx):
        pass

    def build_gold(self, ctx):
        pass


class _SilverBoom(_Ok):
    name = "boom_pipe"

    def build_silver(self, ctx):
        raise RuntimeError("silver exploded")


def _mk(cls, spark):
    # parquet + flat names: the local-test mode used across the suite
    return cls(spark=spark, schema_enabled=False, table_format="parquet")


def _log_rows(spark, pipeline_name):
    df = spark.read.table("ops_run_log")
    return sorted(((r.step, r.status) for r in df.where(df.pipeline == pipeline_name).collect()))


def test_successful_run_logs_every_step(spark):
    _mk(_Ok, spark).run("2025-11-17")
    rows = _log_rows(spark, "ok_pipe")
    assert rows == [("gold", "Succeeded"), ("ingest", "Succeeded"), ("silver", "Succeeded")]
    df = spark.read.table("ops_run_log")
    r = df.where((df.pipeline == "ok_pipe") & (df.step == "gold")).first()
    assert r.run_date == "2025-11-17"
    assert r.duration_s >= 0.0
    assert r.error is None


def test_failed_step_is_logged_and_reraised(spark):
    with pytest.raises(RuntimeError, match="silver exploded"):
        _mk(_SilverBoom, spark).run("2025-11-18")
    rows = _log_rows(spark, "boom_pipe")
    # ingest succeeded, silver failed, gold never ran
    assert rows == [("ingest", "Succeeded"), ("silver", "Failed")]
    df = spark.read.table("ops_run_log")
    r = df.where((df.pipeline == "boom_pipe") & (df.step == "silver")).first()
    assert "silver exploded" in r.error


def test_with_ingest_false_skips_ingest_row(spark):
    p = _mk(_Ok, spark)
    p.name = "no_ingest_pipe"
    p.logger.name = "x"
    p.run("2025-11-19", with_ingest=False)
    rows = _log_rows(spark, "no_ingest_pipe")
    assert rows == [("gold", "Succeeded"), ("silver", "Succeeded")]
