from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql.functions import col, max, min, first, last, sum, avg, lit, stddev, to_date, year, month
from pyspark.sql.types import StructType, StructField, DecimalType, LongType, DateType
from datetime import datetime
from typing import Optional

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
    .config("spark.sql.catalog.spark_catalog.warehouse", "s3a://investment/delta") \
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


def create_tables(
    spark,
    path="s3a://investment/delta",
    database: str = "Investment"
):
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {database}")

    # Drop the existing table if it exists
    spark.sql(f"DROP TABLE IF EXISTS {database}.StockQuotesBronze")
    
    spark.sql(f"DROP TABLE IF EXISTS {database}.AssetPerformanceSilver")
    spark.sql(f"DROP TABLE IF EXISTS {database}.MarketTrendAnalysisSilver")
    
    # Drop the existing Gold layer table if it exists
    spark.sql(f"DROP TABLE IF EXISTS {database}.AssetPerformanceSummaryGold")
    
    # Create the new StockQuotesBronze table
    spark.sql(
        f"""
           CREATE TABLE {database}.StockQuotesBronze (
                    Date DATE,
                    Ticker STRING,
                    Open DECIMAL(38, 10),
                    High DECIMAL(38, 10),
                    Low DECIMAL(38, 10),
                    Close DECIMAL(38, 10),
                    AdjClose DECIMAL(38, 10),
                    Volume BIGINT
                ) USING DELTA
                LOCATION '{path}/{database}/StockQuotesBronze';
            """
    )


    # Asset Performance Table (Silver Layer)

    spark.sql(
            f"""
            CREATE TABLE {database}.AssetPerformanceSilver (
                Date DATE,
                Ticker STRING,
                OpeningPrice DECIMAL(38, 10),
                ClosingPrice DECIMAL(38, 10),
                HighPrice DECIMAL(38, 10),
                LowPrice DECIMAL(38, 10),
                AverageVolume BIGINT,
                IsActive BOOLEAN,
                Version INT,
                ValidFrom DATE,
                ValidTo DATE
            ) USING DELTA
            LOCATION '{path}/{database}/AssetPerformanceSilver';
        """
    )

    # Market Trend Analysis Table (Silver Layer)

    spark.sql(
        f"""
        CREATE TABLE {database}.MarketTrendAnalysisSilver (
            Date DATE,
            Ticker STRING,
            TotalMarketVolume BIGINT,
            MarketOpening DECIMAL(38, 10),
            MarketClosing DECIMAL(38, 10),
            MarketHigh DECIMAL(38, 10),
            MarketLow DECIMAL(38, 10),
            IsActive BOOLEAN,
            Version INT,
            ValidFrom DATE,
            ValidTo DATE
        ) USING DELTA
        LOCATION '{path}/{database}/MarketTrendAnalysisSilver';
        """
    )


    
    spark.sql(
        f"""
           CREATE TABLE {database}.AssetPerformanceSummaryGold (
                Year INT,
                Month INT,
                Ticker STRING,
                AverageROI DECIMAL(38, 10),
                Volatility DECIMAL(38, 10)
            ) USING DELTA
            LOCATION '{path}/{database}/AssetPerformanceSummaryGold';
        """
    )

class StandardETL:
    def __init__(self, storage_path: Optional[str] = None, database: Optional[str] = None, partition: Optional[str] = None):
        self.STORAGE_PATH = storage_path or 's3a://investment/delta'
        self.DATABASE = database or 'investment'
        self.DEFAULT_PARTITION = partition or datetime.now().strftime("%Y-%m-%d-%H-%M-%S")

spark = SparkSession.builder.appName("investment_pipelines").config("spark.sql.ansi.enabled", "false").enableHiveSupport().getOrCreate()
etl = StandardETL()

def load_csv_to_bronze(spark, file_name, table_name, ticker):
    #file_path = f"/opt/bitnami/spark/work-dir/investment/pipelines/{file_name}"
    file_path=f"mage_demo/data/finance/{file_name}"
    schema = StructType([
       # StructField("Date", Char(10), True),
        StructField("Date", DateType(), True),
        StructField("Open", DecimalType(38, 10), True),
        StructField("High", DecimalType(38, 10), True),
        StructField("Low", DecimalType(38, 10), True),
        StructField("Close", DecimalType(38, 10), True),
        StructField("AdjClose", DecimalType(38, 10), True),
        StructField("Volume", LongType(), True)
    ])
        
    df = spark.read.csv(file_path, header=True, schema=schema)
    df = df.withColumn("Ticker", lit(ticker))

    # Debugging: Print the DataFrame schema
    df.printSchema()
    # Debugging: Show sample data
    df.show(5)
    
    # Write DataFrame to Bronze table
    save_path = f"s3a://investment/delta/Investment/StockQuotesBronze"
    print(save_path)
    #df.write.format("delta").option("mergeSchema", "true").mode("overwrite").saveAsTable(table_name)
    df.write.format("delta").mode("overwrite").save("s3a://investment/delta/Investment/StockQuotesBronze")


def transform_to_silver(spark, bronze_table, asset_silver_table, market_silver_table):
    bronze_df = spark.table(bronze_table)

    converted_df = bronze_df.withColumn("Open", col("Open").cast(DecimalType(38, 10))) \
                        .withColumn("High", col("High").cast(DecimalType(38, 10))) \
                        .withColumn("Low", col("Low").cast(DecimalType(38, 10))) \
                        .withColumn("Close", col("Close").cast(DecimalType(38, 10)))

    asset_df = converted_df.groupBy("Date", "Ticker").agg(
        max("High").alias("HighPrice"),
        min("Low").alias("LowPrice"),
        first("Open").alias("OpeningPrice"),
        last("Close").alias("ClosingPrice"),
        avg("Volume").cast("long").alias("AverageVolume")
    ).withColumn("IsActive", lit(True))\
     .withColumn("Version", lit(1))\
     .withColumn("ValidFrom", col("Date"))\
     .withColumn("ValidTo", to_date(lit("2099-01-01"), "yyyy-MM-dd"))
    
    save_path = f"s3a://investment/delta/Investment/AssetPerformanceSilver"
    print(save_path)
    asset_df.write.format("delta").option("mergeSchema", "true").mode("overwrite").save(save_path)

    market_df = converted_df.groupBy("Date", "Ticker").agg(
        sum("Volume").alias("TotalMarketVolume"),
        avg("Open").cast(DecimalType(38, 10)).alias("MarketOpening"),
        avg("Close").cast(DecimalType(38, 10)).alias("MarketClosing"),
        max("High").cast(DecimalType(38, 10)).alias("MarketHigh"),
        min("Low").cast(DecimalType(38, 10)).alias("MarketLow")
    ).withColumn("IsActive", lit(True))\
     .withColumn("Version", lit(1))\
     .withColumn("ValidFrom", col("Date"))\
     .withColumn("ValidTo", to_date(lit("2099-01-01"), "yyyy-MM-dd"))
     
    save_path = f"s3a://investment/delta/Investment/MarketTrendAnalysisSilver"
    print(save_path)
    market_df.write.format("delta").option("mergeSchema", "true").mode("overwrite").save(save_path)

def create_gold_layer(spark, asset_silver_table, gold_table):
    asset_df = spark.table(asset_silver_table)

    # Créer des colonnes pour l'année et le mois
    asset_df = asset_df.withColumn("Year", year("Date")).withColumn("Month", month("Date"))

    # Grouper par année, mois et Ticker
    gold_df = asset_df.groupBy("Year", "Month", "Ticker").agg(
        avg("ClosingPrice").cast(DecimalType(38, 10)).alias("AverageROI"),
        stddev("ClosingPrice").cast(DecimalType(38, 10)).alias("Volatility")
    )

    # Écrire dans la table Gold
    save_path = f"s3a://investment/delta/Investment/AssetPerformanceSummaryGold"
    print(save_path)
    gold_df.write.format("delta").option("mergeSchema", "true").mode("overwrite").save(save_path)
csv_files_and_tickers = {
    "AAPL.csv": "AAPL",
    "AMZN.csv": "AMZN",
    "GOOG.csv": "GOOG",
    "MSFT.csv": "MSFT",
    "ORCL.csv": "ORCL"
}

# Debugging: Print the types of the keys and values in the dictionary
for file_name, ticker in csv_files_and_tickers.items():
   print(f"File: {file_name} (Type: {type(file_name)}), Ticker: {ticker} (Type: {type(ticker)})")

# Call the functions for ETL process
for file_name, ticker in csv_files_and_tickers.items():
    load_csv_to_bronze(spark, file_name, f"{etl.DATABASE}.StockQuotesBronze", ticker)
 #   load_csv_to_bronze(spark, "investment.StockQuotesBronze")

#transform_to_silver(spark, "Investment.StockQuotesBronze", "Investment.AssetPerformanceSilver", "Investment.MarketTrendAnalysisSilver")

#create_gold_layer(spark, "Investment.AssetPerformanceSilver", "Investment.AssetPerformanceSummaryGold")

# Close Spark session
spark.stop()

if __name__ == '__main__':
    spark = (
        SparkSession.builder.appName("investment_models")
        .config("spark.executor.cores", "1")
        .config("spark.executor.instances", "1")
        .enableHiveSupport()
        .getOrCreate()
    )
    create_tables(spark)