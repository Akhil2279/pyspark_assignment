
def reorder_cols(df):
    return df.select("employee_id","employee_name","salary","state","age","department")