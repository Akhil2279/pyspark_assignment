
import pytest
from src.question_2.ques_1.data_frame import get_spark, get_credit_card_df
from src.question_2.ques_2.util import get_partition_count
from src.question_2.ques_3.util import increase_partitions
from src.question_2.ques_4.util import decrease_partitions
from src.question_2.ques_5.util import mask_card
from pyspark.sql.functions import col


@pytest.fixture(scope="module")
def spark():
    spark = get_spark("test_q2")
    yield spark
    spark.stop()

# Test 1
def test_credit_card_count(spark):
    df = get_credit_card_df(spark)
    assert df.count() == 5

# Test 2

def test_partition_count(spark):
    df = get_credit_card_df(spark)
    count = get_partition_count(df)
    assert count >= 1   # default partitions

# Test 3

def test_increase_partitions(spark):
    df = get_credit_card_df(spark)
    df = increase_partitions(df)
    assert df.rdd.getNumPartitions() == 5

# Test 4

def test_decrease_partitions(spark):
    df = get_credit_card_df(spark)
    original = df.rdd.getNumPartitions()

    df = increase_partitions(df)
    df = decrease_partitions(df, original)

    assert df.rdd.getNumPartitions() == original

# Test 5

def test_masking(spark):
    df = get_credit_card_df(spark)

    mask_udf = mask_card()
    df = df.withColumn("masked", mask_udf(col("card_number")))

    sample = df.collect()[0]

    assert sample["masked"].startswith("************")
    assert len(sample["masked"]) == 16