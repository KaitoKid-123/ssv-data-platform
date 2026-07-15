"""Static config shared by all pipelines."""

# Lakehouse layer names. With a schema-enabled Lakehouse these are SQL schemas
# (bronze.sale_bill); otherwise the PipelineContext flattens them (bronze_sale_bill).
BRONZE = "bronze"
SILVER = "silver"
GOLD = "gold"

# Source timestamps are stored UTC; the VN business day is UTC+7.
VN_TZ_OFFSET_HOURS = 7