from src.question_2.ques_1.data_frame import get_spark, get_credit_card_df
from src.question_2.ques_2.util import get_partition_count

def main():
    spark = get_spark("partition_count")
    df = get_credit_card_df(spark)

    partition_count = get_partition_count(df)
    print("Original Partitions:", partition_count)

if __name__ == "__main__":
    main()