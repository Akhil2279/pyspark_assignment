
from pyspark.sql.functions import explode

def flatten_df(df):
    return df.select("*", explode("items").alias("item"))