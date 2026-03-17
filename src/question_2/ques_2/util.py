def get_partition_count(df):
    return df.rdd.getNumPartitions()