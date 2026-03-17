from src.question_3.ques_1.data_frame import get_user_activity_df , get_spark
from src.question_3.ques_2.util import rename_columns

def main():
    spark = get_spark("rename_columns")

    df = get_user_activity_df(spark)

    df = rename_columns(df)
    df.show()

if __name__ == "__main__":
    main()