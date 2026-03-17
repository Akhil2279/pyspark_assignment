
import pytest
from src.question_5.ques_1.data_frame import (
    get_employee_df,
    get_country_df
)
from src.question_5.ques_2.util import avg_salary
from src.question_5.ques_4.util import add_bonus
from src.question_5.ques_7.util import replace_state


@pytest.fixture(scope="module")
def spark():
    from pyspark.sql import SparkSession
    spark = SparkSession.builder.appName("test_q5").getOrCreate()
    yield spark
    spark.stop()

# Test 2
def test_employee_count(spark):
    df = get_employee_df(spark)
    assert df.count() == 7

# Test 3
def test_avg_salary(spark):
    df = get_employee_df(spark)
    result = avg_salary(df)

    assert "avg(salary)" in result.columns

# Test 4
def test_bonus_column(spark):
    df = get_employee_df(spark)
    df = add_bonus(df)

    assert "bonus" in df.columns

# Test 7
def test_replace_state(spark):
    emp = get_employee_df(spark)
    country = get_country_df(spark)

    df = replace_state(emp, country)

    assert "country_name" in df.columns