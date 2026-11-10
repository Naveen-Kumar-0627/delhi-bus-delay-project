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
from pyspark.sql import functions as F
from pyspark.sql.types import *


Silver_Table="C:/Bus_Data/Tables/Silver_Table/"
Check_Path="C:/Bus_Data/Tables/Checks/Check1/"
Bronze_Table="C:/Bus_Data/Tables/Bronze_Table/"
schema=StructType([
    StructField("vehicle_id",StringType()),
    StructField("trip_id",StringType()), 
    StructField("start_time",StringType()),
    StructField("state_date",StringType()),
    StructField("start_date",StringType()),
    StructField("schedule_relationship",IntegerType()),
    StructField("route_id",StringType()),
    StructField("latitude",DoubleType()),
    StructField("longitude",DoubleType()),
    StructField("speed",DoubleType()),
    StructField("pulled_at",LongType()),
    StructField("ingest_at",LongType()),
    StructField("event_ts",LongType())
])
print("Reading from Bronze Table")
bronze_df=(spark.readStream.format('delta').load(Bronze_Table)\
.withColumn("j",F.from_json(F.col("raw_json"),schema))\
         .select("j.*","partition","offset")\
        .withColumn("event_ts",F.timestamp_seconds("event_ts"))\
        .withColumn("pulled_at",F.timestamp_seconds("pulled_at"))\
        .withColumn("ingest_at",F.timestamp_seconds("ingest_at"))\
        .withColumn("raw_start_date",F.coalesce(F.col("state_date"),F.col("start_date"))))
final_df=bronze_df.withColumn("trip_start_ts",
        F.try_to_timestamp(F.concat_ws(" ",F.col("raw_start_date"),F.col("start_time")),
        F.lit("yyyyMMdd HH:mm:ss")))\
        .withColumn("start_date",F.to_date(F.col("raw_start_date"),"yyyyMMdd"))\
        .drop("start_time","state_date","raw_start_date")
        
print("Read and Transformations are Completed")
print("Writing to Silver table")         
final_df.writeStream\
         .format('delta')\
         .option("checkpointLocation",Check_Path)\
         .trigger(processingTime="7 minutes")\
         .start(Silver_Table)\
         .awaitTermination()
print("Write completed")         