# Databricks notebook source
# MAGIC %run "/Users/harishmurugan9952@gmail.com/Src/source_bronze/util"

# COMMAND ----------

employee_df = spark.read.format("delta").load('dbfs:/FileStore/assignments/question1/silver/employee_info/dim_employees')
display(employee_df)

# COMMAND ----------

salary_of_department = employee_with_date_df.orderBy(desc('salary'))
display(salary_of_department.select('department', 'salary'))

# COMMAND ----------

employee_count = employee_df.groupBy("department", "country").agg(count("employeeid").alias("employee_count"))
display(employee_count)

# COMMAND ----------

department_join_df = employee_with_date_df.join(department_with_date_df, employee_with_date_df.department == department_with_date_df.department_i_d, "inner")
country_join_df = department_join_df.join(country_with_date_df, department_join_df.country == country_with_date_df.country_code, "inner")
department_with_country = country_join_df.select('department_name', 'country_name')
display(department_with_country)


# COMMAND ----------

avg_age_employee = employee_with_date_df.groupBy('department').agg(avg("age").alias('avg_age'))
display(avg_age_employee)

# COMMAND ----------

employee_with_date_df.write.format("parquet").mode("overwrite").option("replaceWhere", "load_date = '2024-04-16'").save("/FileStore/assignments/gold/employee/table_name")

# COMMAND ----------

employee_with_date_df.write.format("parquet").mode("overwrite").option("replaceWhere", "load_date = '2024-04-16'").save("/FileStore/assignments/gold/employee/table_name")
display(employee_with_date_df)
