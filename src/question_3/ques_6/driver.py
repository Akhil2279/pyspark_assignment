
from src.question_3.ques_1.data_frame import get_user_activity_df , get_spark
from src.question_3.ques_6.util import write_managed_table

def main():
    spark = get_spark("managed_table")

    df = get_user_activity_df(spark)

    write_managed_table(df)

if __name__ == "__main__":
    main()