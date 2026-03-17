
from src.question_3.ques_1.data_frame import get_user_activity_df , get_spark
from src.question_3.ques_3.util import last_7_days_activity

def main():
    spark = get_spark("last_7_days")

    df = get_user_activity_df(spark)

    result = last_7_days_activity(df)
    result.show()

if __name__ == "__main__":
    main()