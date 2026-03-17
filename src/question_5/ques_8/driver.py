
from src.question_5.ques_1.data_frame import get_employee_df, get_country_df , get_spark
from src.question_5.ques_7.util import replace_state
from src.question_5.ques_8.util import lower_and_date

def main():
    spark = get_spark("lower")

    emp = get_employee_df(spark)
    country = get_country_df(spark)

    df = replace_state(emp, country)
    df = lower_and_date(df)

    df.show()

if __name__ == "__main__":
    main()