
from pyspark.sql.functions import year, month, dayofmonth

def add_date_parts(df):
    return df.withColumn("year", year("load_date")) \
             .withColumn("month", month("load_date")) \
             .withColumn("day", dayofmonth("load_date"))