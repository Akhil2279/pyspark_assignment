
def increase_partitions(df):
    return df.repartition(5)