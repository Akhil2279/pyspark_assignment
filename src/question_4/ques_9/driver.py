
from src.question_4.ques_1.data_frame import read_json_df , get_spark
from src.question_4.ques_7.util import add_load_date
from src.question_4.ques_8.util import add_date_parts
from src.question_4.ques_9.util import write_partition_table

def main():
    spark = get_spark("write_table")

    df = read_json_df(spark, "path_to_json")

    df = add_load_date(df)
    df = add_date_parts(df)

    write_partition_table(df)

if __name__ == "__main__":
    main()