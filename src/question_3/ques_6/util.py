
def write_managed_table(df):
    df.write.mode("overwrite").saveAsTable("user.login_details")