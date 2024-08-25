from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

# Paths to JARs - Ensure these paths are correct
delta_jar = "/home/mage_code/mage_demo/spark-config/delta-core_2.12-2.4.0.jar"
delta_s_jar = "/home/mage_code/mage_demo/spark-config/delta-storage-2.4.0.jar"
hadoop_aws_jar = "/home/mage_code/mage_demo/spark-config/hadoop-aws-3.3.4.jar"
aws_sdk_jar = "/home/mage_code/mage_demo/spark-config/aws-java-sdk-bundle-1.12.262.jar"

# Initialize Spark Session with Delta and MinIO configurations

spark = SparkSession.builder \
    .appName("DeltaExample1") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.sql.catalog.spark_catalog.type", "hadoop") \
    .config("spark.sql.catalog.spark_catalog.warehouse", "s3a://delta-datalake/warehouse") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "minio") \
    .config("spark.hadoop.fs.s3a.secret.key", "minio123") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.jars", f"{delta_jar},{delta_s_jar},{hadoop_aws_jar},{aws_sdk_jar}") \
    .config("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", "2") \
    .config("spark.hadoop.mapreduce.fileoutputcommitter.cleanup-failures.ignored", "true") \
    .getOrCreate()

# Set log level for debugging
#spark.sparkContext.setLogLevel("DEBUG")

# Create the schema (database) if it does not exist
#spark.sql("CREATE DATABASE IF NOT EXISTS airbnb")

# Use the schema
#spark.sql("USE airbnb")

# Load your data (assuming you have a CSV file for this example)
file_path = "/home/mage_code/mage_demo/data/listings.csv"
df = spark.read.csv(file_path, header=True, inferSchema=True)
df.show(20)

print('process listings')

split_cols = F.split(df['name'], '·')
     
is_review_present = F.trim(split_cols.getItem(1)).startswith('★')

# Extract, clean and assign new columns
df = df.withColumn('description', F.trim(split_cols.getItem(0))) \
        .withColumn('reviews', F.when(is_review_present, F.trim(F.regexp_replace(split_cols.getItem(1), '★', ''))).otherwise(None)) \
        .withColumn('bedrooms', F.when(is_review_present, F.trim(split_cols.getItem(2))).otherwise(F.trim(split_cols.getItem(1)))) \
        .withColumn('beds', F.when(is_review_present, F.trim(split_cols.getItem(3))).otherwise(F.trim(split_cols.getItem(2)))) \
        .withColumn('baths', F.when(is_review_present, F.trim(split_cols.getItem(4))).otherwise(F.trim(split_cols.getItem(3))))
                    
df = df.drop('name', 'neighbourhood_group', 'license')
df.show(20)

table_name="airbnb1.listings1"

# Insert data into the Delta table
#df.write.format("delta").mode("overwrite").saveAsTable(table_name)

#spark.sql("create database airbnb1")

# Insert data locally 
#df.write.format("delta").mode("append").saveAsTable(table_name)

# Define the S3 path directly for writing
s3_path = "s3a://delta-datalake/warehouse/airbnb/listings"

# Write DataFrame to Delta table
#df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(table_name)
df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(s3_path)

# Verify that the table has been created
spark.sql("SHOW TABLES").show()

print(f"Data inserted into {table_name} successfully.")

# Verify the data was written to the correct S3 path
print("Data written to:", s3_path)

# Stop the Spark session when done
spark.stop()
