
from pyspark.sql.functions import current_date

def lower_and_date(df):
    df = df.toDF(*[c.lower() for c in df.columns])
    return df.withColumn("load_date", current_date())