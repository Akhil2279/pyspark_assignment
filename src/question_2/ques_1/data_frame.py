from pyspark.sql import SparkSession

from pyspark.sql.types import StructType, StructField, StringType

def get_spark(session_name="app"):
    return SparkSession.builder.appName(session_name).getOrCreate()

def get_credit_card_df(spark):
    schema = StructType([
        StructField("card_number", StringType(), True)
    ])

    data = [
        ("1234567891234567",),
        ("5678912345671234",),
        ("9123456712345678",),
        ("1234567812341122",),
        ("1234567812341342",)
    ]

    return spark.createDataFrame(data, schema)