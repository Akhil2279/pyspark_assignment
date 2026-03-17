import pytest
from src.question_3.ques_1.data_frame import get_user_activity_df
from src.question_3.ques_3.util import last_7_days_activity
from src.question_3.ques_4.util import add_login_date


@pytest.fixture(scope="module")
def spark():
    from pyspark.sql import SparkSession
    spark = SparkSession.builder.appName("test_q3").getOrCreate()
    yield spark
    spark.stop()

# Test 2

def test_dataframe_count(spark):
    df = get_user_activity_df(spark)
    assert df.count() == 8

# Test 3

def test_columns(spark):
    df = get_user_activity_df(spark)
    assert "user_id" in df.columns

# Test 4

def test_login_date_column(spark):
    df = get_user_activity_df(spark)
    df = add_login_date(df)

    assert "login_date" in df.columns

# Test 5
def test_last_7_days_logic(spark):
    df = get_user_activity_df(spark)
    result = last_7_days_activity(df)

    assert "user_id" in result.columns