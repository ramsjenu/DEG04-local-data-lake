from pyspark.sql import SparkSession
from pyspark.sql.functions import col, max, min, first, last, sum, avg, lit, stddev, to_date, year, month
from pyspark.sql.types import StructType, StructField, DecimalType, LongType, DateType
from datetime import datetime
from typing import Optional

class StandardETL:
    def __init__(self, storage_path: Optional[str] = None, database: Optional[str] = None, partition: Optional[str] = None):
        self.STORAGE_PATH = storage_path or 's3a://investment/delta'
        self.DATABASE = database or 'fin_invest'
        self.DEFAULT_PARTITION = partition or datetime.now().strftime("%Y-%m-%d-%H-%M-%S")

spark = SparkSession.builder.appName("investment_pipelines").config("spark.sql.ansi.enabled", "false").enableHiveSupport().getOrCreate()
etl = StandardETL()

def load_csv_to_bronze(spark, file_name, table_name, ticker):
    file_path = f"mage_demo/data/finance/{file_name}"
    database="fin_invest"
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
    save_path = f"s3a://investment/delta/{database}/{table_name}"
    print(save_path)
    #df.write.format("delta").option("mergeSchema", "true").mode("overwrite").saveAsTable(table_name)
    df.write.format("delta").mode("overwrite").save(save_path)

def transform_to_silver(spark, bronze_table, asset_silver_table, market_silver_table):
    database="fin_invest"
    bronze_df = spark.table(f"{database}.{bronze_table}")

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
    
    save_path = f"s3a://investment/delta/{database}/{asset_silver_table}"
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
     
    save_path = f"s3a://investment/delta/{database}/{market_silver_table}"
    print(save_path)
    market_df.write.format("delta").option("mergeSchema", "true").mode("overwrite").save(save_path)

def create_gold_layer(spark, asset_silver_table, gold_table):
    database="fin_invest"
    asset_df = spark.table(f"{database}.{asset_silver_table}")
    
    # Créer des colonnes pour l'année et le mois
    asset_df = asset_df.withColumn("Year", year("Date")).withColumn("Month", month("Date"))

    # Grouper par année, mois et Ticker
    gold_df = asset_df.groupBy("Year", "Month", "Ticker").agg(
        avg("ClosingPrice").cast(DecimalType(38, 10)).alias("AverageROI"),
        stddev("ClosingPrice").cast(DecimalType(38, 10)).alias("Volatility")
    )

    # Écrire dans la table Gold
    save_path = f"s3a://investment/delta/{database}/{gold_table}"
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
   # load_csv_to_bronze(spark, file_name, f"{etl.DATABASE}.StockQuotesBronze", ticker)
   load_csv_to_bronze(spark, file_name, "StockQuotesBronze", ticker)

transform_to_silver(spark, "StockQuotesBronze", "AssetPerformanceSilver", "MarketTrendAnalysisSilver")

create_gold_layer(spark, "AssetPerformanceSilver", "AssetPerformanceSummaryGold")

# Close Spark session
spark.stop()
