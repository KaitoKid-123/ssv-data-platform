"""write_delta option wiring — stub df/writer, no Spark session needed.

Regression for the SCD2 migration failure on Fabric: overwriting silver.dim_* with
the 3 new SCD2 columns raised DELTA_0007 (schema mismatch) because write_delta never
set overwriteSchema.
"""
from ssv_data.io.writers import write_delta


class _Writer:
    def __init__(self):
        self.opts = {}

    def format(self, f):
        self.opts["format"] = f
        return self

    def mode(self, m):
        self.opts["mode"] = m
        return self

    def option(self, k, v):
        self.opts[k] = v
        return self

    def partitionBy(self, c):
        self.opts["partitionBy"] = c
        return self

    def saveAsTable(self, t):
        self.opts["saved_table"] = t


class _Schema:
    fields = []          # no void columns


class _DF:
    schema = _Schema()

    def __init__(self):
        self.write = _Writer()


class _Catalog:
    @staticmethod
    def tableExists(t):
        return True


class _Spark:
    catalog = _Catalog()


class _Ctx:
    table_format = "delta"
    logger = None
    spark = _Spark()


def test_default_overwrite_does_not_touch_schema():
    df = _DF()
    write_delta(_Ctx(), df, "silver.some_table")
    assert "overwriteSchema" not in df.write.opts
    assert df.write.opts["mode"] == "overwrite"
    assert df.write.opts["saved_table"] == "silver.some_table"


def test_overwrite_schema_flag_sets_the_delta_option():
    df = _DF()
    write_delta(_Ctx(), df, "silver.dim_product", overwrite_schema=True)
    assert df.write.opts["overwriteSchema"] == "true"


def test_replace_where_still_set_when_table_exists():
    df = _DF()
    write_delta(_Ctx(), df, "gold.fact", partition_col="report_date",
                replace_where="report_date = '2025-11-29'")
    assert df.write.opts["replaceWhere"] == "report_date = '2025-11-29'"
    assert df.write.opts["partitionBy"] == "report_date"
