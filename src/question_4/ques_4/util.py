
from pyspark.sql.functions import explode, explode_outer, posexplode

def explode_df(df):
    return df.select(explode("items"))

def explode_outer_df(df):
    return df.select(explode_outer("items"))

def posexplode_df(df):
    return df.select(posexplode("items"))