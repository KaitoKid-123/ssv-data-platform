"""Shared demographic decodes — graduated to ssv_data under the rule of two
(used by eod_sale_product silver and eod_sale_service silver)."""
import pytest
from pyspark.sql import SparkSession

from ssv_data.transforms.demographics import decode_customer_demographics


@pytest.fixture(scope="module")
def spark():
    s = (SparkSession.builder.master("local[2]").appName("demo-tests")
         .config("spark.sql.shuffle.partitions", "2")
         .config("spark.ui.enabled", "false")
         .getOrCreate())
    yield s
    s.stop()


def _df(spark, rows):
    return spark.createDataFrame(rows, "customer_gender string, customer_age_range string, customer_nationality string")


def test_decodes_known_codes(spark):
    df = _df(spark, [("1", "2", "0"), ("2", "3", "1"), ("1", "5", "2"), ("2", "5", "3")])
    out = {(r.customer_gender, r.customer_age_range, r.customer_nationality)
           for r in decode_customer_demographics(df).collect()}
    assert out == {("Male", "Student", "Vietnamese"), ("Female", "Middle", "Asian"),
                   ("Male", "Foreigner", "US_UK"), ("Female", "Foreigner", "Other")}


def test_unknown_codes_default_to_empty(spark):
    df = _df(spark, [("9", "7", "42"), (None, None, None)])
    for r in decode_customer_demographics(df).collect():
        assert (r.customer_gender, r.customer_age_range, r.customer_nationality) == ("", "", "")


def test_age_passthrough_mode_keeps_unknown_values(spark):
    # eod_sale_service legacy keeps the ORIGINAL value for unmapped age codes
    df = _df(spark, [("1", "7", "0")])
    r = decode_customer_demographics(df, keep_unknown_age=True).first()
    assert r.customer_age_range == "7"


def test_numeric_typed_columns_are_handled(spark):
    # sources sometimes deliver these as ints — decode must not depend on string typing
    df = spark.createDataFrame([(1, 2, 0)], "customer_gender int, customer_age_range int, customer_nationality int")
    r = decode_customer_demographics(df).first()
    assert (r.customer_gender, r.customer_age_range, r.customer_nationality) == ("Male", "Student", "Vietnamese")
