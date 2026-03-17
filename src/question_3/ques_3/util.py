

from pyspark.sql.functions import to_timestamp, expr

def last_7_days_activity(df):
    df = df.withColumn("time_stamp", to_timestamp("time_stamp"))

    return df.filter(
        expr("time_stamp >= current_timestamp() - interval 7 days")
    ).groupBy("user_id").count()