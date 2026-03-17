
from pyspark.sql.functions import to_timestamp, to_date

def add_login_date(df):
    df = df.withColumn("time_stamp", to_timestamp("time_stamp"))
    return df.withColumn("login_date", to_date("time_stamp"))