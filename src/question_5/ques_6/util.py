
def join_types(emp, dept):
    inner = emp.join(dept, emp.department == dept.dept_id, "inner")
    left = emp.join(dept, emp.department == dept.dept_id, "left")
    right = emp.join(dept, emp.department == dept.dept_id, "right")
    return inner, left, right