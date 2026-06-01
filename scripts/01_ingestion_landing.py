# Databricks notebook source
from pyspark.sql import functions as F

landing_path = "/Volumes/nyc-yellow/default/landing"

files = [
    "yellow_tripdata_2023-01.parquet",
    "yellow_tripdata_2023-02.parquet",
    "yellow_tripdata_2023-03.parquet",
    "yellow_tripdata_2023-04.parquet",
    "yellow_tripdata_2023-05.parquet"
]

def read_and_standardize(file_name):
    df = spark.read.parquet(f"{landing_path}/{file_name}")

    return (
        df.select(
            F.col("VendorID").cast("int").alias("vendor_id"),
            F.col("passenger_count").cast("int").alias("passenger_count"),
            F.col("total_amount").cast("double").alias("total_amount"),
            F.col("tpep_pickup_datetime").cast("timestamp").alias("pickup_datetime"),
            F.col("tpep_dropoff_datetime").cast("timestamp").alias("dropoff_datetime")
        )
        .withColumn("source_file", F.lit(file_name))
    )

df_list = [read_and_standardize(file) for file in files]

df_landing = df_list[0]
for df in df_list[1:]:
    df_landing = df_landing.unionByName(df)

display(df_landing.limit(10))