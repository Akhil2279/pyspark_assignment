
from src.question_5.ques_1.data_frame import get_employee_df , get_spark
from src.question_5.ques_4.util import add_bonus

def main():
    spark = get_spark("bonus")

    df = get_employee_df(spark)

    add_bonus(df).show()

if __name__ == "__main__":
    main()