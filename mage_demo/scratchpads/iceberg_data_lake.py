import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

# Paths to JARs - Ensure these paths are correct
iceberg_jar = "/home/mage_code/mage_demo/spark-config/iceberg-spark-runtime-3.5_2.12-1.5.2.jar"
hadoop_aws_jar = "/home/mage_code/mage_demo/spark-config/hadoop-aws-3.3.4.jar"
aws_sdk_jar = "/home/mage_code/mage_demo/spark-config/aws-java-sdk-bundle-1.12.262.jar"

files = [iceberg_jar, hadoop_aws_jar, aws_sdk_jar]

for file in files:
    if os.path.exists(file):
        print(f"File found: {file}")
    else:
        print(f"File not found: {file}")


# Initialize Spark Session with Iceberg and MinIO configurations

spark = SparkSession.builder \
    .appName("IcebergExample") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.spark_catalog.type", "hadoop") \
    .config("spark.sql.catalog.spark_catalog.warehouse", "s3a://iceberg-datalake/warehouse") \
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "minio") \
    .config("spark.hadoop.fs.s3a.secret.key", "minio123") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.jars", f"{iceberg_jar},{hadoop_aws_jar},{aws_sdk_jar}") \
    .config("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", "2") \
    .config("spark.hadoop.mapreduce.fileoutputcommitter.cleanup-failures.ignored", "true") \
    .getOrCreate()

# Set log level for debugging
#spark.sparkContext.setLogLevel("DEBUG")

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

table_name="airbnb.listings"

# Insert data into the Iceberg table
df.write.format("iceberg").mode("overwrite").option("overwriteSchema", "true").saveAsTable(table_name)

# Insert data locally 
#df.write.format("csv").mode("overwrite").saveAsTable(table_name)

print(f"Data inserted into {table_name} successfully.")

# Stop the Spark session when done
spark.stop()
