
def write_csv(df, path):
    df.write.mode("overwrite").option("header", True).csv(path)