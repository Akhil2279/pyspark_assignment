
from src.question_4.ques_1.data_frame import read_json_df ,get_spark
from src.question_4.ques_6.util import to_snake_case

def main():
    spark = get_spark("snake_case")

    df = read_json_df(spark, "path_to_json")

    df = to_snake_case(df)
    df.show()

if __name__ == "__main__":
    main()