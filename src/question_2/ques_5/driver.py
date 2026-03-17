
from src.question_2.ques_1.data_frame import get_spark, get_credit_card_df
from src.question_2.ques_5.util import mask_card
from pyspark.sql.functions import col

def main():
    spark = get_spark("mask_card")
    df = get_credit_card_df(spark)

    mask_udf = mask_card()

    df = df.withColumn("masked_card_number", mask_udf(col("card_number")))
    df.show()

if __name__ == "__main__":
    main()