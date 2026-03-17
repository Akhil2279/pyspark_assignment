
import pytest
from src.question_1.data_frame import get_spark, purchase_data, product_data
from src.question_1.ques_2.util import only_iphone13
from src.question_1.ques_3.util import upgraded_to_iphone14
from src.question_1.ques_4.util import bought_all_products


# Spark fixture
@pytest.fixture(scope="module")
def spark():
    spark = get_spark("test_q1")
    yield spark
    spark.stop()


# Test 1
def test_purchase_count(spark):
    df = purchase_data(spark)
    assert df.count() == 11


# Test 2
def test_product_count(spark):
    df = product_data(spark)
    assert df.count() == 5


# Test 3
def test_purchase_columns(spark):
    df = purchase_data(spark)
    assert df.columns == ["customer", "product_model"]


# Test 4
def test_only_iphone13(spark):
    df = purchase_data(spark)
    result = only_iphone13(df).collect()

    customers = [row["customer"] for row in result]

    assert customers == [4]


# Test 5
def test_upgraded_customers(spark):
    df = purchase_data(spark)
    result = upgraded_to_iphone14(df).collect()

    customers = sorted([row["customer"] for row in result])

    assert customers == [1, 3]

# Test 6
def test_all_products(spark):
    purchase_df = purchase_data(spark)
    product_df = product_data(spark)

    result = bought_all_products(purchase_df, product_df).collect()

    customers = [row["customer"] for row in result]

    assert customers == [1]
