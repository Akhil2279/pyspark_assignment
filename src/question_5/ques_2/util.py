
def avg_salary(df):
    return df.groupBy("department").avg("salary")