
from pyspark.sql.functions import current_date

def add_load_date(df):
    return df.withColumn("load_date", current_date())