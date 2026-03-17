
def decrease_partitions(df, num_partitions):
    return df.coalesce(num_partitions)