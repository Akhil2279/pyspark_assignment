from pyspark.sql.functions import col, collect_set, array_contains

def upgraded_to_iphone14(df):
    agg_df = df.groupBy("customer") \
        .agg(collect_set("product_model").alias("products"))

    return agg_df.filter(
        array_contains(col("products"), "iphone13") &
        array_contains(col("products"), "iphone14")
    ).select("customer")