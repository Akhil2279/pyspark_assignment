
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

def mask_card():
    def mask(card):
        return "*" * 12 + card[-4:]
    return udf(mask, StringType())