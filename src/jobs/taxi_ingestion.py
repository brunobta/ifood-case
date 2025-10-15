from pyspark.sql import SparkSession
from pyspark.sql.functions import col, month, year, to_timestamp, concat_ws
from pyspark.sql.types import StructType, StructField, LongType, DoubleType, StringType, TimestampType, IntegerType
from pyspark.dbutils import DBUtils
from functools import reduce
from delta.tables import DeltaTable

from src.utils.data_quality import apply_data_quality_checks


def run_ingestion_job(spark: SparkSession, input_path: str, data_lake_path: str):
    """
    Executa o pipeline de ingestão de dados de táxis de NY.

    Este pipeline lê dados brutos em formato parquet, os ingere em uma
    camada Bronze (Delta) e, em seguida, os processa e armazena em uma
    camada Silver (Delta) com dados limpos e transformados.

    :param spark: A sessão Spark ativa.
    :param input_path: O caminho para os dados brutos (landing zone).
    :param data_lake_path: O caminho base para salvar as tabelas Delta.
    """
    bronze_table_path = f"{data_lake_path}/bronze/yellow_taxi_trips"
    silver_table_path = f"{data_lake_path}/silver/yellow_taxi_trips"
    quarantine_table_path = f"{data_lake_path}/quarantine/yellow_taxi_trips"

    # Abordagem robusta para ler múltiplos arquivos com esquemas inconsistentes.
    # 1. Lista todos os arquivos na landing zone.
    # 2. Lê cada arquivo individualmente.
    # 3. Padroniza os nomes das colunas para cada DataFrame.
    # 4. Une todos os DataFrames em um só.
    print(f"Lendo dados brutos de: {input_path}")
    dbutils = DBUtils(spark)
    files = dbutils.fs.ls(input_path)
    parquet_files = [f.path for f in files if f.path.endswith(".parquet")]

    all_dfs = []
    for file_path in parquet_files:
        print(f"Processando arquivo: {file_path}")
        df = spark.read.parquet(file_path)
        # Padroniza os nomes das colunas para snake_case para este arquivo
        for column in df.columns:
            new_column_name = column.lower().replace(' ', '_')
            df = df.withColumnRenamed(column, new_column_name)
        all_dfs.append(df)

    if not all_dfs:
        raise ValueError(f"Nenhum arquivo parquet encontrado em {input_path}")

    # Une todos os dataframes usando unionByName para lidar com ordens de colunas diferentes
    standardized_df = reduce(lambda df1, df2: df1.unionByName(df2), all_dfs)

    # --- Camada Bronze ---
    print(f"Iniciando escrita na camada Bronze em: {bronze_table_path}")
    # O modo "overwrite" aqui é para garantir a idempotência do job em um cenário de teste.
    standardized_df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(bronze_table_path)

    bronze_table = DeltaTable.forPath(spark, bronze_table_path)
    bronze_records_count = bronze_table.history(1).select("operationMetrics.numOutputRows").collect()[0][0]
    print(f"{bronze_records_count} registros escritos na camada Bronze.")
    spark.sql(f"CREATE TABLE IF NOT EXISTS taxi_bronze USING DELTA LOCATION '{bronze_table_path}'")
    print("Tabela 'taxi_bronze' criada/atualizada.")

    # --- Camada Silver ---
    bronze_df = spark.read.format("delta").load(bronze_table_path)

    transformed_df = bronze_df.select(
        col("vendorid").alias("vendor_id").cast(IntegerType()),
        col("tpep_pickup_datetime").alias("pickup_datetime"),
        col("tpep_dropoff_datetime").alias("dropoff_datetime"),
        col("passenger_count").cast(IntegerType()),
        col("total_amount").cast(DoubleType())
    )

    print("Aplicando regras de qualidade de dados...")
    dq_checked_df = apply_data_quality_checks(transformed_df)

    dq_checked_df.cache()

    good_records_df = dq_checked_df.filter(col("dq_status") == "PASS").drop("dq_status", "dq_failures")
    quarantined_records_df = dq_checked_df.filter(col("dq_status") == "FAIL")

    quarantined_count = quarantined_records_df.count()

    silver_df = good_records_df.withColumn("pickup_month", month(col("pickup_datetime"))) \
                               .withColumn("pickup_year", year(col("pickup_datetime")))

    print(f"Iniciando escrita na camada Silver em: {silver_table_path}")
    silver_df.write.format("delta") \
             .mode("overwrite") \
             .option("overwriteSchema", "true") \
             .partitionBy("pickup_year", "pickup_month") \
             .save(silver_table_path)

    silver_table = DeltaTable.forPath(spark, silver_table_path)
    silver_records_count = silver_table.history(1).select("operationMetrics.numOutputRows").collect()[0][0]
    print(f"{silver_records_count} registros escritos na camada Silver.")
    spark.sql(f"CREATE TABLE IF NOT EXISTS taxi_silver USING DELTA LOCATION '{silver_table_path}'")
    print("Tabela 'taxi_silver' criada/atualizada.")

    print(f"Encontrados {quarantined_count} registros para a quarentena.")
    if quarantined_count > 0:
        print(f"Escrevendo registros na camada de Quarentena em: {quarantine_table_path}")
        # Converte o array de falhas em uma string para facilitar a consulta
        quarantined_records_df.withColumn("dq_failures", concat_ws(",", col("dq_failures"))) \
            .write.format("delta").mode("overwrite").save(quarantine_table_path)
        spark.sql(f"CREATE TABLE IF NOT EXISTS taxi_quarantine USING DELTA LOCATION '{quarantine_table_path}'")
        print("Tabela 'taxi_quarantine' criada/atualizada.")

    dq_checked_df.unpersist()

    print("Pipeline de ingestão concluído com sucesso!")