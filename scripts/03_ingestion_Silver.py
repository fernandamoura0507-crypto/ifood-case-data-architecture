# Databricks notebook source
from pyspark.sql import functions as F

# ============================================================
# CAMADA SILVER -
# Objetivo:
# Aplicar regras de qualidade, padronização, enriquecimento
# e filtro de escopo temporal do case: Jan/2023 a Mai/2023.
# ============================================================

bronze_table = "`nyc-yellow`.default.bronze_taxi_trip"
silver_table = "`nyc-yellow`.default.silver_taxi_trip"

df_bronze = spark.table(bronze_table)

df_silver = (
    df_bronze
    .select(
        F.col("vendor_id").cast("int").alias("vendor_id"),
        F.col("passenger_count").cast("int").alias("passenger_count"),
        F.col("total_amount").cast("double").alias("total_amount"),
        F.col("pickup_datetime").cast("timestamp").alias("pickup_datetime"),
        F.col("dropoff_datetime").cast("timestamp").alias("dropoff_datetime"),
        F.col("source_file")
    )

    # Remove registros com campos obrigatórios nulos
    .filter(F.col("vendor_id").isNotNull())
    .filter(F.col("passenger_count").isNotNull())
    .filter(F.col("total_amount").isNotNull())
    .filter(F.col("pickup_datetime").isNotNull())
    .filter(F.col("dropoff_datetime").isNotNull())

    # Mantém somente o período solicitado no case: Jan/2023 a Mai/2023
    .filter(
        (F.col("pickup_datetime") >= F.to_timestamp(F.lit("2023-01-01 00:00:00"))) &
        (F.col("pickup_datetime") <= F.to_timestamp(F.lit("2023-05-31 23:59:59")))
    )

    # Regras básicas de qualidade
    .filter(F.col("passenger_count") > 0)
    .filter(F.col("total_amount") > 0)
    .filter(F.col("dropoff_datetime") > F.col("pickup_datetime"))

    # Colunas derivadas para análise
    .withColumn("pickup_date", F.to_date("pickup_datetime"))
    .withColumn("pickup_month", F.date_format("pickup_datetime", "yyyy-MM"))
    .withColumn("pickup_hour", F.hour("pickup_datetime"))
)

# Gravação da Silver ajustada
(
    df_silver
    .write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(silver_table)
)

print(f"Tabela Silver recriada com sucesso: {silver_table}")

# Validação
df_silver_validation = spark.table(silver_table)

print(f"Quantidade de registros na Bronze: {df_bronze.count():,}")
print(f"Quantidade de registros na Silver ajustada: {df_silver_validation.count():,}")
print(f"Quantidade de colunas na Silver ajustada: {len(df_silver_validation.columns)}")

display(df_silver_validation.limit(10))

df_silver_validation.printSchema()