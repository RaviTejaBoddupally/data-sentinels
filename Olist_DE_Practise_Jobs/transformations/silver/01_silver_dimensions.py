import dlt
from pyspark.sql import functions as F

spark.sql("USE CATALOG data_sentinals")
spark.sql("USE SCHEMA silver")



# -------------------------------------------------------------------
# CONFIG
# Update these only if your Bronze catalog/schema/table names differ.
# -------------------------------------------------------------------
BRONZE_GEO_TABLE = "data_sentinals.bronze.raw_geolocation"
BRONZE_CUSTOMERS_TABLE = "data_sentinals.bronze.raw_customers"
BRONZE_SELLERS_TABLE = "data_sentinals.bronze.raw_olist_sellers"


# -------------------------------------------------------------------
# 1) GEO ZIP PREFIX REFERENCE
# Purpose:
# - Standardize geolocation data
# - Collapse duplicate zip-prefix rows
# - Prepare a safe lookup table for customer/seller enrichment
# -------------------------------------------------------------------
@dlt.table(
    name="geo_zip_prefix_silver",
    comment="Standardized geolocation lookup"
)
@dlt.expect_or_drop("zip_not_null", "zip_code_prefix IS NOT NULL")
@dlt.expect("valid_state", "length(geo_state) = 2")
def geo_zip_prefix_silver():

    geo = spark.read.table(BRONZE_GEO_TABLE)

    geo_std = (
        geo.select(
            F.col("geolocation_zip_code_prefix").cast("int").alias("zip_code_prefix"),
            F.lower(F.trim("geolocation_city")).alias("geo_city"),
            F.upper(F.trim("geolocation_state")).alias("geo_state"),
            F.col("geolocation_lat").cast("double").alias("lat"),
            F.col("geolocation_lng").cast("double").alias("lng")
        )
    )
 # Olist geolocation has many rows per zip prefix.
    # Build one representative record using averages for lat/lng
    # and a representative city/state.
    return (
        geo_std.groupBy("zip_code_prefix", "geo_state")
        .agg(
            F.avg("lat").alias("avg_lat"),
            F.avg("lng").alias("avg_lng"),
            F.first("geo_city", ignorenulls=True).alias("geo_city")
        )
    )  


# -------------------------------------------------------------------
# 2) CUSTOMER GEO DIMENSION
# Purpose:
# - Standardize customer geography
# - Enrich with representative lat/lng from zip prefix
# -------------------------------------------------------------------
@dlt.table(
    name="customer_geo_silver",
    comment="Customer dimension with geo enrichment"
)
@dlt.expect_or_drop("customer_id_not_null", "customer_id IS NOT NULL")
def customer_geo_silver():

    customers = spark.read.table(BRONZE_CUSTOMERS_TABLE)
    geo = dlt.read("geo_zip_prefix_silver")

    customers_std = (
        customers.select(
            "customer_id",
            "customer_unique_id",
            F.col("customer_zip_code_prefix").cast("int"),
            F.lower(F.trim("customer_city")).alias("customer_city_std"),
            F.upper(F.trim("customer_state")).alias("customer_state")
        )
    )

    return (
        customers_std.alias("c")
        .join(
            geo.alias("g"),
            (F.col("c.customer_zip_code_prefix") == F.col("g.zip_code_prefix")) &
            (F.col("c.customer_state") == F.col("g.geo_state")),
            "left"
        )
        .select(
            "c.*",
            F.col("g.avg_lat").alias("customer_lat"),
            F.col("g.avg_lng").alias("customer_lng")
        )
    )

# -------------------------------------------------------------------
# 3) SELLER GEO DIMENSION
# Purpose:
# - Standardize seller geography
# - Enrich with representative lat/lng from zip prefix
# -------------------------------------------------------------------
@dlt.table(
    name="seller_geo_silver",
    comment="Seller dimension with geo enrichment"
)
@dlt.expect_or_drop("seller_id_not_null", "seller_id IS NOT NULL")
def seller_geo_silver():

    sellers = spark.read.table(BRONZE_SELLERS_TABLE)
    geo = dlt.read("geo_zip_prefix_silver")

    sellers_std = (
        sellers.select(
            "seller_id",
            F.col("seller_zip_code_prefix").cast("int"),
            F.lower(F.trim("seller_city")).alias("seller_city_std"),
            F.upper(F.trim("seller_state")).alias("seller_state")
        )
    )

    return (
        sellers_std.alias("s")
        .join(
            geo.alias("g"),
            (F.col("s.seller_zip_code_prefix") == F.col("g.zip_code_prefix")) &
            (F.col("s.seller_state") == F.col("g.geo_state")),
            "left"
        )
        .select(
            "s.*",
            F.col("g.avg_lat").alias("seller_lat"),
            F.col("g.avg_lng").alias("seller_lng")
        )
    )