
from src.question_5.ques_1.data_frame import get_employee_df, get_country_df ,get_spark
from src.question_5.ques_7.util import replace_state

def main():
    spark = get_spark("country")

    emp = get_employee_df(spark)
    country = get_country_df(spark)

    replace_state(emp, country).show()

if __name__ == "__main__":
    main()