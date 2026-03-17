
def write_external(df):
    df.write.mode("overwrite").format("parquet").save("output/parquet_table")
    df.write.mode("overwrite").format("csv").save("output/csv_table")