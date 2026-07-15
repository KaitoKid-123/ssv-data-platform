"""Explicit schemas for nested Mongo fields.

Declaring these up front replaces every defensive `if col not in df.columns`
in the original pandas job: a missing field simply parses to null.
"""
from pyspark.sql.types import (
    ArrayType, BooleanType, DoubleType, LongType, StringType, StructField, StructType,
)

SALE_ITEM_SCHEMA = ArrayType(StructType([
    StructField("productCode", StringType()),
    StructField("productName", StringType()),
    StructField("uom", StringType()),
    StructField("uomSize", LongType()),
    StructField("quantity", DoubleType()),
    StructField("totalAmount", DoubleType()),
    StructField("totalAmountWithoutTax", DoubleType()),
    StructField("totalAmountByPromotion", DoubleType()),
    StructField("totalAmountWithoutTaxByPromotion", DoubleType()),
    StructField("winPromotion", BooleanType()),
    StructField("productGroupId", LongType()),
    StructField("productGroup", StringType()),
    StructField("productSubCategoryId", LongType()),
    StructField("productSubCategory", StringType()),
    StructField("productCategoryId", LongType()),
    StructField("productCategory", StringType()),
    StructField("productUomId", LongType()),
    StructField("retailSellingPrice", DoubleType()),
    StructField("retailBusinessType", StringType()),
    StructField("purchasePriceWithTax", DoubleType()),
    StructField("purchasePriceWithoutTax", DoubleType()),
    StructField("outputVatCode", StringType()),
    # TODO: add remaining saleNormalItems fields consumed downstream
]))

PROMOTION_SCHEMA = ArrayType(StructType([
    StructField("promotionCode", StringType()),
    StructField("promotionName", StringType()),
    StructField("totalDiscountAmount", DoubleType()),
    StructField("voucherType", LongType()),
    StructField("voucherCode", StringType()),
    StructField("products", StringType()),
]))