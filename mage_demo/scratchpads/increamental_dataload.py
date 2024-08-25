from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
from pyspark.sql.functions import col, coalesce,expr

# Create a Spark session
spark = SparkSession.builder.appName("IncrementalProcessing").getOrCreate()

# Define the schema
schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("age", IntegerType(), True),
    StructField("salary", IntegerType(), True)
])

# Define the schema for incremental data
schema_incremental = StructType([
    StructField("inc_id", IntegerType(), True),
    StructField("inc_name", StringType(), True),
    StructField("inc_age", IntegerType(), True),
    StructField("inc_salary", IntegerType(), True)
])

# Create the initial DataFrame
initial_data = [
    (1, "Alice", 30, 50000),
    (2, "Bob", 35, 60000),
    (3, "Carol", 28, 45000)
]

df = spark.createDataFrame(initial_data, schema=schema)

# Show the initial DataFrame
df.show()

# Create another DataFrame with incremental data (including new and changed records)
incremental_data = [
    (1, "Alice", 30, 52000),   # Updated salary for Alice
    (2, "Bob", 29, 65000),     # Updated salary for Bob
    (3, "Hari", 28, 47000),   # Update Name,age and salary for Hari
    (4, "Dave", 40, 70000)     # New record for Dave
]

# Create a new DataFrame with the incremental data
incremental_df = spark.createDataFrame(incremental_data, schema=schema_incremental)
# Show the incremental DataFrame
incremental_df.show()
# Identify changed records (based on 'id') in the incremental data
changed_records = df.join(incremental_df, col("id") == col("inc_id"), 'inner').filter(
    (df.salary != incremental_df.inc_salary) |
    (df.name != incremental_df.inc_name) |
    (df.age != incremental_df.inc_age)
).select("inc_id","inc_name","inc_age","inc_salary")

# Show the changed records
print("Changed Records:")
changed_records.show()

# Identify new records in the incremental data
new_records = incremental_df.join(df, col("id") == col("inc_id"), 'left_outer').filter(df.id.isNull()).select("inc_id","inc_name","inc_age","inc_salary")

# Show the new records
print("New Records:")
new_records.show()



# Update the existing records in the original DataFrame with the changes
updated_df = df.join(
    changed_records.selectExpr("inc_id as id", "inc_name ", "inc_age", "inc_salary"),
    'id',
    'left_outer'
).coalesce(1).\
    withColumn("name", coalesce(col("inc_name"), col("name"))).\
    withColumn("age", coalesce(col("inc_age"), col("age"))).\
    withColumn("salary", coalesce(col("inc_salary"), col("salary"))).\
    drop("inc_name", "inc_age", "inc_salary")

# Show the updated DataFrame
print("Updated DataFrame:")
updated_df.show()


# Combine the updated DataFrame with the new records
final_df = updated_df.union(new_records)

# Show the final DataFrame
print("Final DataFrame:")
final_df.show()

# Stop the Spark session
spark.stop()

