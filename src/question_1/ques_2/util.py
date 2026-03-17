from pyspark.sql.functions import col, collect_set, size, array_contains

def only_iphone13(df):
    agg_df = df.groupBy("customer") \
        .agg(collect_set("product_model").alias("products"))

    return agg_df.filter(
        (size(col("products")) == 1) &
        (array_contains(col("products"), "iphone13"))
    ).select("customer")