# Databricks notebook source
from pyspark.sql import functions as F

# ============================================================
# CAMADA BRONZE
# Objetivo:
# Gravar os dados consolidados da Landing Zone em uma tabela
# gerenciada no Databricks, preservando a granularidade original.
# ============================================================

# Como o catálogo possui hífen no nome, usamos crase.
bronze_table = "`nyc-yellow`.default.bronze_taxi_trip"

# Gravação da tabela Bronze
(
    df_landing
    .write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(bronze_table)
)

print(f"Tabela Bronze criada com sucesso: {bronze_table}")

# ============================================================
# VALIDAÇÃO DA CAMADA BRONZE
# ============================================================

df_bronze = spark.table(bronze_table)

print(f"Quantidade de registros na Bronze: {df_bronze.count():,}")
print(f"Quantidade de colunas na Bronze: {len(df_bronze.columns)}")

display(df_bronze.limit(10))

df_bronze.printSchema()