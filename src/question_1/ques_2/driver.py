from src.question_1.data_frame import purchase_data, get_spark
from src.question_1.ques_2.util import only_iphone13

def main():
    spark = get_spark(session_name="only_iphone13")
    df = purchase_data(spark)

    only_iphone13(df).show()

if __name__ == "__main__":
    main()