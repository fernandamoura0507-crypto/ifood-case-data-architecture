# Databricks notebook source
from pyspark.sql import functions as F

df_gold = spark.table("`nyc-yellow`.default.gold_taxi_trip")

avg_total_amount_month = (
    df_gold
    .groupBy("pickup_month")
    .agg(
        F.round(
            F.avg("total_amount"),
            2
        ).alias("avg_total_amount")
    )
    .orderBy("pickup_month")
)

display(avg_total_amount_month)