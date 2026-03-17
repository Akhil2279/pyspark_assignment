
from src.question_4.ques_1.data_frame import read_json_df , get_spark
from src.question_4.ques_7.util import add_load_date

def main():
    spark = get_spark("load_date")

    df = read_json_df(spark, "path_to_json")

    df = add_load_date(df)
    df.show()

if __name__ == "__main__":
    main()