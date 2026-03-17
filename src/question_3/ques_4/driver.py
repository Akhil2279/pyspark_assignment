
from src.question_3.ques_1.data_frame import get_user_activity_df , get_spark
from src.question_3.ques_4.util import add_login_date

def main():
    spark = get_spark("login_date")

    df = get_user_activity_df(spark)

    df = add_login_date(df)
    df.show()

if __name__ == "__main__":
    main()