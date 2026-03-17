from src.question_2.ques_1.data_frame import get_spark, get_credit_card_df
from src.question_2.ques_4.util import decrease_partitions

def main():
    spark = get_spark("decrease_partition")
    df = get_credit_card_df(spark)

    original = df.rdd.getNumPartitions()

    df = df.repartition(5)
    df = decrease_partitions(df, original)

    print("Partitions after decrease:", df.rdd.getNumPartitions())

if __name__ == "__main__":
    main()