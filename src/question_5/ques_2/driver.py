
from src.question_5.ques_1.data_frame import get_employee_df, get_spark
from src.question_5.ques_2.util import avg_salary

def main():
    spark = get_spark("avg_salary")

    df = get_employee_df(spark)

    avg_salary(df).show()

if __name__ == "__main__":
    main()