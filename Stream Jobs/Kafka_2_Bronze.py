import os
os.environ["HADOOP_HOME"] = r"C:\hadoop"
os.environ["PATH"] = r"C:\hadoop\bin;" + os.environ.get("PATH", "")

import findspark
findspark.init()

import pyspark
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from delta import configure_spark_with_delta_pip

v = pyspark.__version__
scala = "2.13" if v.startswith("4") else "2.12"
kafka_pkg = f"org.apache.spark:spark-sql-kafka-0-10_{scala}:{v}"

builder = (
    SparkSession.builder
    .master("local[*]")
    .appName("MyApp")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .config("spark.hadoop.fs.file.impl", "org.apache.hadoop.fs.LocalFileSystem")
    .config("spark.hadoop.fs.file.impl.disable.cache", "true")
    .config("spark.jars.ivy", "C:/Bus_Data/ivy")
)

spark = configure_spark_with_delta_pip(builder, extra_packages=[kafka_pkg]).getOrCreate()
print("Spark Session created:", spark.version)
 
Bronze_Table="C:/Bus_Data/Tables/Bronze_Table/"
Check_Path="C:/Bus_Data/Tables/Checks/Check2/"
from pyspark.sql import functions as F
from pyspark.sql.types import *

BOOTSTRAP="localhost:29092"
TOPIC="BUS"
 
print("Reading from kafka") 
raw=(spark.readStream.format("kafka")\
    .option("kafka.bootstrap.servers",BOOTSTRAP)\
    .option("subscribe",TOPIC)\
    .option("startingOffsets","earliest")\
    .option("failOnDataLoss","false")\
     .load()
    )
print("Read completed")    
bronze=(raw.select (F.col("key").cast("string").alias("kafka_key"),
    F.col("value").cast("string").alias("raw_json"),
    "partition",
     "offset",
     "topic",
    F.col("timestamp").alias("kafka_ts")))
print("Writing to Bronze Table") 
q=(bronze.writeStream.format('delta')\
        .option("checkpointLocation",Check_Path)\
        .trigger(processingTime="7 minutes")\
        .start(Bronze_Table))
q.awaitTermination()
print("Write completed")