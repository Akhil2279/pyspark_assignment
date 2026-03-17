
from src.question_3.ques_1.data_frame import get_user_activity_df , get_spark
from src.question_3.ques_5.util import write_csv

def main():
    spark = get_spark("write_csv")

    df = get_user_activity_df(spark)

    write_csv(df, "output/user_activity_csv")

if __name__ == "__main__":
    main()