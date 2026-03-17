from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, ArrayType

def get_spark(session_name):
    return SparkSession.builder.appName(session_name).getOrCreate()

def get_employee_df(spark):

    schema = StructType([
        StructField("id", StringType(), True),
        StructField("name", StringType(), True),
        StructField("items", ArrayType(
            StructType([
                StructField("product", StringType(), True),
                StructField("price", StringType(), True)
            ])
        ), True)
    ])

    data = [
        ("0001", "Akhil", [
            {"product": "iphone13", "price": "800"},
            {"product": "dell i5 core", "price": "700"}
        ]),
        ("0002", "Rahul", [
            {"product": "iphone14", "price": "900"}
        ]),
        ("0003", "Suresh", None)  # to test explode_outer
    ]

    return spark.createDataFrame(data, schema)


def read_json_df(spark, path):
    return spark.read.option("multiline", True).json(path)

# Create Spark Session
spark = get_spark("EmployeeApp")

# Create DataFrame
df = get_employee_df(spark)

# Show Data
df.show(truncate=False)