
def write_partition_table(df):
    df.write.mode("overwrite") \
        .partitionBy("year", "month", "day") \
        .format("json") \
        .saveAsTable("employee.employee_details")