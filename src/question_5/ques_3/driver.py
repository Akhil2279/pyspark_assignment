
from src.question_5.ques_1.data_frame import get_employee_df, get_department_df , get_spark
from src.question_5.ques_3.util import employees_with_m


def main():
    spark = get_spark("filter_m")

    emp = get_employee_df(spark)
    dept = get_department_df(spark)

    employees_with_m(emp, dept).show()

if __name__ == "__main__":
    main()