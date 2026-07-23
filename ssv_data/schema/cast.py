"""Schema-driven fill + cast (replaces Util.util.fill_missing_column / clean_data_by_schema).
 
A `schema` is a dict {column: type_token}. Type tokens match the original job:
  'str' | 'int' | 'float' | 'date' | 'list' | 'list_from_str'
"""
from pyspark.sql import functions as F
from pyspark.sql.types import (
    ArrayType, BooleanType, DoubleType, LongType, StringType, TimestampType,
)

_TYPE_MAP = {
    "str": StringType(),
    "int": LongType(),
    "float": DoubleType(),
    "date": TimestampType(),
    "bool": BooleanType(),
}
 
 
def fill_missing_columns(df, schema: dict):
    """Add any column in `schema` missing from df, as a typed null (or empty array)."""
    for col, t in schema.items():
        if col not in df.columns:
            if t in ("list", "list_from_str"):
                df = df.withColumn(col, F.array().cast(ArrayType(StringType())))
            else:
                df = df.withColumn(col, F.lit(None).cast(_TYPE_MAP.get(t, StringType())))
    return df
 
 
def cast_by_schema(df, schema: dict):
    """Cast existing columns to their target Spark types (best-effort)."""
    for col, t in schema.items():
        if col not in df.columns:
            continue
        if t == "list_from_str":
            df = df.withColumn(col, F.split(F.coalesce(F.col(col).cast("string"), F.lit("")), ","))
        elif t == "list":
            df = df.withColumn(col, F.col(col).cast(ArrayType(StringType())))
        else:
            df = df.withColumn(col, F.col(col).cast(_TYPE_MAP.get(t, StringType())))
    return df
 
 
def select_schema(df, schema: dict):
    """Project df to exactly the schema's columns, in order."""
    return df.select(*schema.keys())