
from pyspark.sql.functions import col

def add_bonus(df):
    return df.withColumn("bonus", col("salary") * 2)