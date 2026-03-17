
from src.question_4.ques_1.data_frame import read_json_df , get_spark
from src.question_4.ques_2.util import flatten_df


def main():
    spark = get_spark("flatten")

    df = read_json_df(spark, "path_to_json")

    flat_df = flatten_df(df)
    flat_df.show()

if __name__ == "__main__":
    main()