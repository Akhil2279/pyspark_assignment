
from src.question_5.ques_1.data_frame import get_employee_df , get_spark
from src.question_5.ques_5.util import reorder_cols

def main():
    spark = get_spark("reorder")

    df = get_employee_df(spark)

    reorder_cols(df).show()

if __name__ == "__main__":
    main()