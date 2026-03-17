
def replace_state(emp, country):
    return emp.join(country, emp.state == country.country_code) \
        .select("employee_id","employee_name","department","country_name","salary","age")