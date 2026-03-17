
from pyspark.sql.functions import col

def employees_with_m(emp_df, dept_df):
    return emp_df.join(dept_df, emp_df.department == dept_df.dept_id) \
        .filter(col("employee_name").startswith("m")) \
        .select("employee_name","dept_name")