from pyspark.sql.functions import col, collect_set, size

def bought_all_products(purchase_df, product_df):
    total_products = product_df.count()

    agg_df = purchase_df.groupBy("customer") \
        .agg(collect_set("product_model").alias("products"))

    return agg_df.filter(
        size(col("products")) == total_products
    ).select("customer")