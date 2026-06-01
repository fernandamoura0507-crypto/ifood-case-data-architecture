# Databricks notebook source
from pyspark.sql import functions as F

df_gold = spark.table("`nyc-yellow`.default.gold_taxi_trip")

avg_passengers_hour = (
    df_gold
    .filter(F.col("pickup_month") == "2023-05")
    .groupBy("pickup_hour")
    .agg(
        F.round(
            F.avg("passenger_count"),
            2
        ).alias("avg_passenger_count")
    )
    .orderBy("pickup_hour")
)

display(avg_passengers_hour)