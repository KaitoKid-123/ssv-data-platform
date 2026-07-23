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
# ---- eod_sale_service (Pay Bill / Top Up / Pay Card) ----
# One SUPERSET item schema for paybillItems / topUpItems / payCardItems: fields a
# given service lacks (e.g. purchase prices on Pay Bill) simply parse to null.
SERVICE_ITEM_SCHEMA = ArrayType(StructType([
    StructField("productCode", StringType()),
    StructField("productName", StringType()),
    StructField("productUomId", StringType()),
    StructField("supplierCode", StringType()),
    StructField("supplierName", StringType()),
    StructField("quantity", LongType()),
    StructField("purchasePriceWithTax", DoubleType()),
    StructField("purchasePriceWithoutTax", DoubleType()),
    StructField("totalAmount", DoubleType()),
    StructField("retailBusinessType", StringType()),
    StructField("commissionOnVnd", DoubleType()),
    StructField("totalCommission", DoubleType()),
]))

# orderInfo is a single nested OBJECT (not an array). Top Up / Pay Card docs carry
# only a subset (no serviceId/customerId) — absent fields parse to null.
SERVICE_ORDER_INFO_SCHEMA = StructType([
    StructField("orderNo", StringType()),
    StructField("serviceId", StringType()),
    StructField("providerId", StringType()),
    StructField("customerId", StringType()),
    StructField("orderType", StringType()),
])
