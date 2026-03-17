
from pyspark.sql.functions import col

def select_required_columns(df):
    return df.select("card_number", "masked_card_number")