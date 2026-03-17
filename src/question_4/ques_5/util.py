
from pyspark.sql.functions import col

def filter_id(df):
    return df.filter(col("id") == "0001")