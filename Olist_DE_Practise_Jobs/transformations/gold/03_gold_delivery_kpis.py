import dlt
from pyspark.sql import functions as F
from pyspark.sql.window import Window


spark.sql("USE CATALOG data_sentinals")
spark.sql("USE SCHEMA gold")


# -------------------------------------------------------------------
# CONFIG
# These are upstream Silver tables, read explicitly from silver schema.
# The pipeline publishing target should be set separately in pipeline settings:
#   Default catalog = data_sentinals
#   Default schema  = gold
# -------------------------------------------------------------------
SILVER_DELIVERY_ORDER_TABLE = "data_sentinals.silver.delivery_order_silverr"
SILVER_DELIVERY_ORDER_ITEM_TABLE = "data_sentinals.silver.delivery_order_item_silver"


# -------------------------------------------------------------------
# GOLD 1: STATE-MONTH DELIVERY PERFORMANCE
# Grain: month x customer_state
# -------------------------------------------------------------------
@dlt.table(
    name="gold_delivery_state_month",
    comment="Monthly delivery KPI summary by customer state"
)
def gold_delivery_state_month():

    delivery = spark.read.table(SILVER_DELIVERY_ORDER_TABLE)

    base = delivery.filter(
        F.col("delivery_delta_days").isNotNull() &
        F.col("customer_state").isNotNull()
    )

    agg_df = (
        base.groupBy("order_purchase_month", "customer_state")
        .agg(
            F.countDistinct("order_id").alias("total_delivered_orders"),
            F.avg("delivery_delta_days").alias("avg_delivery_delta_days"),
            F.expr("percentile_approx(delivery_delta_days, 0.5)").alias("median_delivery_delta_days"),
            F.avg("purchase_to_delivery_days").alias("avg_purchase_to_delivery_days"),
            F.avg("carrier_to_customer_days").alias("avg_carrier_to_customer_days"),
            F.avg("estimated_lead_time_days").alias("avg_estimated_lead_time_days"),
            F.avg(F.when(F.col("is_late") == 1, 1.0).otherwise(0.0)).alias("late_delivery_rate"),
            F.avg(F.when(F.col("delivery_delta_days") <= 0, 1.0).otherwise(0.0)).alias("on_time_or_early_rate"),
            F.avg(F.when(F.col("delivery_delta_days") < 0, 1.0).otherwise(0.0)).alias("early_delivery_rate"),
            F.avg(F.when(F.col("delivery_delta_days") > 7, 1.0).otherwise(0.0)).alias("severe_delay_rate")
        )
    )

    return agg_df.withColumn(
        "state_delay_rank_in_month",
        F.dense_rank().over(
            Window.partitionBy("order_purchase_month")
            .orderBy(F.desc("avg_delivery_delta_days"))
        )
    )


# -------------------------------------------------------------------
# GOLD 2: SELLER-STATE-MONTH DELIVERY PERFORMANCE
# Grain: month x seller_state
# -------------------------------------------------------------------
@dlt.table(
    name="gold_delivery_seller_state_month",
    comment="Monthly delivery KPI summary by seller state"
)
def gold_delivery_seller_state_month():

    delivery = spark.read.table(SILVER_DELIVERY_ORDER_ITEM_TABLE)

    base = delivery.filter(
        F.col("delivery_delta_days").isNotNull() &
        F.col("seller_state").isNotNull()
    )

    agg_df = (
        base.groupBy("order_purchase_month", "seller_state")
        .agg(
            F.countDistinct("order_id").alias("total_orders"),
            F.countDistinct("seller_id").alias("active_sellers"),
            F.avg("delivery_delta_days").alias("avg_delivery_delta_days"),
            F.expr("percentile_approx(delivery_delta_days, 0.5)").alias("median_delivery_delta_days"),
            F.avg("order_seller_item_value").alias("avg_order_seller_item_value"),
            F.avg("order_seller_freight_value").alias("avg_order_seller_freight_value"),
            F.avg(F.when(F.col("is_late") == 1, 1.0).otherwise(0.0)).alias("late_delivery_rate"),
            F.avg(F.when(F.col("delivery_delta_days") > 7, 1.0).otherwise(0.0)).alias("severe_delay_rate")
        )
    )

    return agg_df.withColumn(
        "seller_state_delay_rank_in_month",
        F.dense_rank().over(
            Window.partitionBy("order_purchase_month")
            .orderBy(F.desc("avg_delivery_delta_days"))
        )
    )


# -------------------------------------------------------------------
# GOLD 3: SELLER-TO-CUSTOMER CORRIDOR PERFORMANCE
# Grain: month x seller_state x customer_state
# -------------------------------------------------------------------
@dlt.table(
    name="gold_delivery_corridor_month",
    comment="Monthly delivery KPI summary by seller-state to customer-state corridor"
)
def gold_delivery_corridor_month():

    delivery = spark.read.table(SILVER_DELIVERY_ORDER_ITEM_TABLE)

    base = delivery.filter(
        F.col("delivery_delta_days").isNotNull() &
        F.col("seller_state").isNotNull() &
        F.col("customer_state").isNotNull()
    )

    agg_df = (
        base.groupBy("order_purchase_month", "seller_state", "customer_state")
        .agg(
            F.countDistinct("order_id").alias("total_orders"),
            F.countDistinct("seller_id").alias("active_sellers"),
            F.avg("delivery_delta_days").alias("avg_delivery_delta_days"),
            F.expr("percentile_approx(delivery_delta_days, 0.5)").alias("median_delivery_delta_days"),
            F.avg("purchase_to_delivery_days").alias("avg_purchase_to_delivery_days"),
            F.avg("order_seller_freight_value").alias("avg_freight_value"),
            F.avg(F.when(F.col("is_late") == 1, 1.0).otherwise(0.0)).alias("late_delivery_rate"),
            F.avg(F.when(F.col("delivery_delta_days") > 7, 1.0).otherwise(0.0)).alias("severe_delay_rate")
        )
    )

    return agg_df.withColumn(
        "corridor_delay_rank_in_month",
        F.dense_rank().over(
            Window.partitionBy("order_purchase_month")
            .orderBy(F.desc("avg_delivery_delta_days"))
        )
    )


# -------------------------------------------------------------------
# GOLD 4: DELAY HOTSPOTS
# Grain: customer_state snapshot
# -------------------------------------------------------------------
@dlt.table(
    name="gold_delivery_hotspots",
    comment="Delay hotspot summary by customer state across all delivered orders"
)
def gold_delivery_hotspots():

    delivery = spark.read.table(SILVER_DELIVERY_ORDER_TABLE)

    base = delivery.filter(
        F.col("delivery_delta_days").isNotNull() &
        F.col("customer_state").isNotNull()
    )

    agg_df = (
        base.groupBy("customer_state")
        .agg(
            F.countDistinct("order_id").alias("total_delivered_orders"),
            F.avg("delivery_delta_days").alias("avg_delivery_delta_days"),
            F.expr("percentile_approx(delivery_delta_days, 0.5)").alias("median_delivery_delta_days"),
            F.avg(F.when(F.col("is_late") == 1, 1.0).otherwise(0.0)).alias("late_delivery_rate"),
            F.avg(F.when(F.col("delivery_delta_days") > 7, 1.0).otherwise(0.0)).alias("severe_delay_rate")
        )
    )

    return agg_df.withColumn(
        "hotspot_rank",
        F.dense_rank().over(Window.orderBy(F.desc("avg_delivery_delta_days")))
    )


# -------------------------------------------------------------------
# GOLD 5: EXECUTIVE SUMMARY
# Grain: one row
# -------------------------------------------------------------------
@dlt.table(
    name="gold_delivery_exec_summary",
    comment="Executive summary of delivery performance KPIs"
)
def gold_delivery_exec_summary():

    delivery = spark.read.table(SILVER_DELIVERY_ORDER_TABLE)

    base = delivery.filter(F.col("delivery_delta_days").isNotNull())

    return (
        base.agg(
            F.countDistinct("order_id").alias("total_delivered_orders"),
            F.avg("delivery_delta_days").alias("avg_delivery_delta_days"),
            F.expr("percentile_approx(delivery_delta_days, 0.5)").alias("median_delivery_delta_days"),
            F.avg("purchase_to_delivery_days").alias("avg_purchase_to_delivery_days"),
            F.avg("carrier_to_customer_days").alias("avg_carrier_to_customer_days"),
            F.avg("estimated_lead_time_days").alias("avg_estimated_lead_time_days"),
            F.avg(F.when(F.col("is_late") == 1, 1.0).otherwise(0.0)).alias("late_delivery_rate"),
            F.avg(F.when(F.col("delivery_delta_days") <= 0, 1.0).otherwise(0.0)).alias("on_time_or_early_rate"),
            F.avg(F.when(F.col("delivery_delta_days") < 0, 1.0).otherwise(0.0)).alias("early_delivery_rate"),
            F.avg(F.when(F.col("delivery_delta_days") > 7, 1.0).otherwise(0.0)).alias("severe_delay_rate")
        )
        .withColumn("kpi_generated_ts", F.current_timestamp())
    )