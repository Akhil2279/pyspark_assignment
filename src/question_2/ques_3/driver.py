
from src.question_2.ques_1.data_frame import get_spark, get_credit_card_df
from src.question_2.ques_3.util import increase_partitions

def main():
    spark = get_spark("increase_partition")
    df = get_credit_card_df(spark)

    df = increase_partitions(df)
    print("Partitions after increase:", df.rdd.getNumPartitions())

if __name__ == "__main__":
    main()