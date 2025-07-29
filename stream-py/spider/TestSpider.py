import pyodbc

# 获取所有可用的 ODBC 驱动
drivers = [x for x in pyodbc.drivers() if x.startswith('ODBC Driver')]
print(drivers)