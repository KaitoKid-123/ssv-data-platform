"""Schema-driven cast helpers — token coverage."""
import pytest
from pyspark.sql import SparkSession

from ssv_data.schema.cast import cast_by_schema, fill_missing_columns, select_schema


@pytest.fixture(scope="module")
def spark():
    s = (SparkSession.builder.master("local[2]").appName("cast-tests")
         .config("spark.sql.shuffle.partitions", "2")
         .config("spark.ui.enabled", "false")
         .getOrCreate())
    yield s
    s.stop()


def test_bool_token_casts_truthy_strings(spark):
    df = spark.createDataFrame([("true",), ("false",), (None,)], "flag string")
    out = cast_by_schema(df, {"flag": "bool"})
    assert dict(out.dtypes)["flag"] == "boolean"
    assert [r.flag for r in out.collect()] == [True, False, None]


def test_bool_token_casts_numeric(spark):
    # Fabric/ClickHouse deliver 0/1 ints for boolean flags — must cast cleanly.
    df = spark.createDataFrame([(1,), (0,)], "flag int")
    out = cast_by_schema(df, {"flag": "bool"})
    assert [r.flag for r in out.collect()] == [True, False]


def test_missing_bool_column_filled_as_typed_null(spark):
    df = spark.createDataFrame([(1,)], "x int")
    out = fill_missing_columns(df, {"present_flag": "bool"})
    assert dict(out.dtypes)["present_flag"] == "boolean"
    assert out.first().present_flag is None


def test_full_roundtrip_projects_and_orders(spark):
    df = spark.createDataFrame([("t01", "1", 5)], "id string, has_invoice string, amt int")
    schema = {"id": "str", "amt": "int", "has_invoice": "bool"}
    out = select_schema(cast_by_schema(fill_missing_columns(df, schema), schema), schema)
    assert out.columns == ["id", "amt", "has_invoice"]
    r = out.first()
    assert (r.id, r.amt, r.has_invoice) == ("t01", 5, True)
