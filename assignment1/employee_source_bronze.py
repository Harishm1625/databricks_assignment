# Databricks notebook source
# MAGIC %run ./util

# COMMAND ----------

emp_read_df=read_csv(emp_read_path)
dept_read_df=read_csv(dept_read_path)
cunt_read_df=read_csv(cunt_read_path)

# COMMAND ----------







# COMMAND ----------

write_csv_file(reading_csv_files,bronze_path)

def overwrite_silver_path(database_name, table_name, df, silver_path):
    
    df.mode("overwrite").option("path",silver_path).saveAsTable(f'{database_name}.{table_name}')



# COMMAND ----------

write_delta_table(employee_df, "Employee_info", "dim_employee", "EmployeeID", "/silver/Employee_info/dim_employee")


# COMMAND ----------

display(dbutils.fs.ls('dbfs:/FileStore/assignments/assignment1/source_to_bronze'))

