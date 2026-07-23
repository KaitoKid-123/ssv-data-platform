"""Shared POS demographic decodes.

Graduated into ssv_data under the rule of two: the same code<->label mapping was
duplicated in eod_sale_product (silver) and the legacy eod_sale_service ETL.

Codes come from the POS as ints or strings depending on the source — comparisons
are done on the stringified value so both work.

Behavioural quirk kept from the legacy jobs: unmapped AGE codes default to ''
(eod_sale_product) or pass through unchanged (eod_sale_service) — pick with
`keep_unknown_age`.
"""
from pyspark.sql import functions as F

GENDER = {"1": "Male", "2": "Female"}
AGE_RANGE = {"2": "Student", "3": "Middle", "5": "Foreigner"}
NATIONALITY = {"0": "Vietnamese", "1": "Asian", "2": "US_UK", "3": "Other"}


def _decode(col, mapping: dict, default):
    c = F.col(col).cast("string")
    expr = default
    for code, label in mapping.items():
        expr = F.when(c == code, F.lit(label)).otherwise(expr)
    return expr


def decode_customer_demographics(df,
                                 gender_col: str = "customer_gender",
                                 age_col: str = "customer_age_range",
                                 nat_col: str = "customer_nationality",
                                 keep_unknown_age: bool = False):
    """Replace raw demographic codes with labels, in place (same column names)."""
    age_default = F.col(age_col).cast("string") if keep_unknown_age else F.lit("")
    return (df
            .withColumn(gender_col, _decode(gender_col, GENDER, F.lit("")))
            .withColumn(age_col, _decode(age_col, AGE_RANGE, age_default))
            .withColumn(nat_col, _decode(nat_col, NATIONALITY, F.lit(""))))
