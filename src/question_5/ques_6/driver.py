
from src.question_5.ques_1.data_frame import get_employee_df, get_department_df , get_spark
from src.question_5.ques_6.util import join_types


def main():
    spark = get_spark("joins")

    emp = get_employee_df(spark)
    dept = get_department_df(spark)

    inner, left, right = join_types(emp, dept)

    inner.show()
    left.show()
    right.show()

if __name__ == "__main__":
    main()