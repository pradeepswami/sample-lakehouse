from datetime import timezone, datetime
from pyspark import pipelines as dp
from datetime import date
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, sum as _sum, count, max as _max, datediff, lit, to_date


def compute_customer_360(
    customers_df: DataFrame,
    orders_df: DataFrame,
    as_of_date: date,
) -> DataFrame:
    customers_current = customers_df.filter(col("__END_AT").isNull())
    orders_current = orders_df.filter(col("__END_AT").isNull())

    order_summary = (
        orders_current
        .groupBy("customer_id")
        .agg(
            _sum("total_amount").alias("lifetime_value"),
            count("order_id").alias("order_count"),
            _max("order_date").alias("last_order_date"),
        )
    )

    return (
        customers_current
        .join(order_summary, "customer_id", "left")
        .withColumn(
            "days_since_last_order",
            datediff(lit(as_of_date), to_date(col("last_order_date"))),
        )
        .select(
            "customer_id", "first_name", "last_name", "loyalty_tier",
            "lifetime_value", "order_count", "last_order_date", "days_since_last_order",
        )
    )



@dp.materialized_view(
    name="gold_customer_360",
    comment="Lifetime value, order frequency, and a descriptive churn signal — Marketing's question from requirement.",
)
def gold_customer_360():
    return compute_customer_360(
        customers_df=spark.read.table("silver_customers"),
        orders_df=spark.read.table("silver_orders"),
        as_of_date=datetime.now(timezone.utc).date(),
    )
