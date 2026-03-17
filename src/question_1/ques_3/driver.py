from src.question_1.data_frame import purchase_data, get_spark
from src.question_1.ques_3.util import upgraded_to_iphone14

def main():
    spark = get_spark(session_name="upgrade_iphone")
    df = purchase_data(spark)

    upgraded_to_iphone14(df).show()

if __name__ == "__main__":
    main()