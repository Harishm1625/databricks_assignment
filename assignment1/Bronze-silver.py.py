# Databricks notebook source
from pyspark.sql.types import *

# employee custome schema
emp_custom_schema = StructType([
    StructField('EmployeeID', IntegerType(), True),
    StructField('EmployeeName', StringType(), True),
    StructField('Department', StringType(), True),
    StructField('Country', StringType(), True),
    StructField('Salary', IntegerType(), True),
    StructField('Age', IntegerType(), True)
])
# country custome schema
cunt_custom_schema = StructType([
    StructField('CountryCode', StringType(), True),
    StructField('CountryName', StringType(), True)
])
# department custome schema
dept_custom_schema = StructType([
    StructField('DepartmentID', StringType(), True),
    StructField('DepartmentName', StringType(), True)
])

emp_df='dbfs:/FileStore/Employee_Q1-1.csv'
dept_df='dbfs:/FileStore/Department_Q1-1.csv'
cunt_df='dbfs:/FileStore/Country_Q1-1.csv'


emp_read_schema=read_schema(emp_df,emp_custom_schema)
display(emp_read_schema)





# COMMAND ----------

employee_snake_case_df = change_column_case_to_snake_case(employee_df)
display(employee_snake_case_df)

department_snake_case_df = change_column_case_to_snake_case(department_df)
display(department_snake_case_df)


country_snake_case_df = change_column_case_to_snake_case(country_df)
display(country_snake_case_df)

# COMMAND ----------


employee_with_date_df = add_current_date(employee_snake_case_df)
display(employee_with_date_df)


department_with_date_df = add_current_date(department_snake_case_df)
display(department_with_date_df)


country_with_date_df = add_current_date(country_snake_case_df)
display(country_with_date_df)



# COMMAND ----------

spark.sql('use employee_info')

# COMMAND ----------

employee_df.write.option('path', 'dbfs:/FileStore/assignments/question1/silver/employee_info/dim_employees').saveAsTable('dim_employees')
