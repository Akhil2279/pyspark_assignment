
import pytest
from src.question_4.ques_1.data_frame import get_employee_df
from src.question_4.ques_2.util import flatten_df
from src.question_4.ques_5.util import filter_id
from src.question_4.ques_7.util import add_load_date
from src.question_4.ques_8.util import add_date_parts


@pytest.fixture(scope="module")
def spark():
    from pyspark.sql import SparkSession
    spark = SparkSession.builder.appName("test_q4").getOrCreate()
    yield spark
    spark.stop()

# Test 1
def test_dataframe_created(spark):
    df = get_employee_df(spark)
    assert df.count() == 3

# Test 2
def test_flatten_increases_rows(spark):
    df = get_employee_df(spark)
    flat_df = flatten_df(df)

    assert flat_df.count() >= df.count()

# Test 5
def test_filter_id(spark):
    df = get_employee_df(spark)
    filtered = filter_id(df)

    result = [row["id"] for row in filtered.collect()]

    assert result == ["0001"]

# Test 7
def test_load_date_added(spark):
    df = get_employee_df(spark)
    df = add_load_date(df)

    assert "load_date" in df.columns

# Test 8
def test_date_parts(spark):
    df = get_employee_df(spark)
    df = add_load_date(df)
    df = add_date_parts(df)

    assert "year" in df.columns
    assert "month" in df.columns
    assert "day" in df.columns