
from src.question_4.ques_1.data_frame import read_json_df , get_spark
from src.question_4.ques_4.util import explode_df, explode_outer_df, posexplode_df

def main():
    spark = get_spark("explode_types")

    df = read_json_df(spark, "path_to_json")

    explode_df(df).show()
    explode_outer_df(df).show()
    posexplode_df(df).show()

if __name__ == "__main__":
    main()