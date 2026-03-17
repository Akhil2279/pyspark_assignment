from src.question_1.data_frame import  get_spark,product_data, purchase_data
from src.question_1.ques_4.util import bought_all_products

def main():
    spark = get_spark(session_name="all_products")

    purchase_df = purchase_data(spark)
    product_df = product_data(spark)

    bought_all_products(purchase_df, product_df).show()

if __name__ == "__main__":
    main()