
from src.question_4.ques_1.data_frame import read_json_df , get_spark
from src.question_4.ques_5.util import filter_id

def main():
    spark = get_spark("filter")

    df = read_json_df(spark, "path_to_json")

    filter_id(df).show()

if __name__ == "__main__":
    main()