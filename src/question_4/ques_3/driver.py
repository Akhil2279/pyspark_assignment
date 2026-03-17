
from src.question_4.ques_1.data_frame import read_json_df , get_spark
from src.question_4.ques_2.util import flatten_df
from src.question_4.ques_3.util import get_counts

def main():
    spark = get_spark("count_diff")

    df = read_json_df(spark, "path_to_json")
    flat_df = flatten_df(df)

    original, flat = get_counts(df, flat_df)

    print("Original:", original)
    print("Flattened:", flat)

if __name__ == "__main__":
    main()