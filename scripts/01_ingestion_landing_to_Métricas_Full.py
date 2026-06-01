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

# COMMAND ----------

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

# COMMAND ----------

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

# COMMAND ----------

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

# COMMAND ----------

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

# COMMAND ----------

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
