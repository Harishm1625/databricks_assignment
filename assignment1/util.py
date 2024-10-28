# Databricks notebook source

# reading csv file function
def read_csv(path):
    df=spark.read.csv(path,header=True,inferSchema=True)
    return df
emp_read_path="dbfs:/FileStore/Employee_Q1.csv"
dept_read_path="dbfs:/FileStore/Department_Q1.csv"
cunt_read_path="dbfs:/FileStore/Country_Q1-1.csv"


emp_read_df=read_csv(emp_read_path)
dept_read_df=read_csv(dept_read_path)
cunt_read_df=read_csv(cunt_read_path)

def write_csv(df,path):
    df.write.format('csv').mode('overwrite').save(path)
    return df

# emp_write_path='testa'
dept_write_path='testb'
cunt_write_path='testc'

emp_write_df=write_csv(emp_read_df,emp_write_path)
dept_write_df=write_csv(dept_read_df,emp_write_path)
cunt_write_df=write_csv(cunt_read_df,emp_write_path)

    
def read_schema(data,schema):
    df.spark.read.csv(path=path,header=False,schema=schema)
    return df

    



# COMMAND ----------

def change_column_case_to_snake_case(df):
    
    def camel_to_snake_case(column_name):
        return ''.join(['_' + c.lower() if c.isupper() else c for c in column_name]).lstrip('_')
    
     for column in df.columns:
        new_column_name = camel_to_snake_case(column)
        df = df.withColumnRenamed(column, new_column_name)
    
    return df

# COMMAND ----------

def write_delta_table(df, database, table, primary_key, path):
    df.write.format("delta") \
        .mode("overwrite") \
        .option("mergeSchema", "true") \
        .option("path", path) \
        .saveAsTable(f"{database}.{table}")
    
    return df


# COMMAND ----------

def read_with_custom_schema(data, schema):
    df = spark.read.csv(data, schema)
    return df

# COMMAND ----------

def read_with_custom_schema_format(data, schema):
    df = spark.read.format('csv').schema(schema).load(data)
    return df

# COMMAND ----------

def add_current_date(df):
    df = df.withColumn("load_date", current_date())
    return df

