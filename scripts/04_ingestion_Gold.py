# Databricks notebook source
from pyspark.sql import functions as F

# ============================================================
# CAMADA GOLD
# Objetivo:
# Disponibilizar a tabela final de consumo analítico,
# baseada na Silver já tratada e restrita ao escopo do case.
# ============================================================

silver_table = "`nyc-yellow`.default.silver_taxi_trip"
gold_table = "`nyc-yellow`.default.gold_taxi_trip"

df_silver = spark.table(silver_table)

df_gold = (
    df_silver
    .select(
        "vendor_id",
        "passenger_count",
        "total_amount",
        "pickup_datetime",
        "dropoff_datetime",
        "pickup_date",
        "pickup_month",
        "pickup_hour"
    )
)

# Gravação da Gold
(
    df_gold
    .write
    .format("delta")
    .mode("overwrite")
    .partitionBy("pickup_month")
    .option("overwriteSchema", "true")
    .saveAsTable(gold_table)
)

print(f"Tabela Gold recriada com sucesso: {gold_table}")

# Validação
df_gold_validation = spark.table(gold_table)

print(f"Quantidade de registros na Gold ajustada: {df_gold_validation.count():,}")
print(f"Quantidade de colunas na Gold ajustada: {len(df_gold_validation.columns)}")

display(df_gold_validation.limit(10))

df_gold_validation.printSchema()