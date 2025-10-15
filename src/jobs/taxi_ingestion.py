from pyspark.sql import SparkSession
from pyspark.sql.functions import col, month, year, to_timestamp, concat_ws
from pyspark.sql.types import IntegerType, DoubleType
from delta.tables import DeltaTable

from utils.data_quality import apply_data_quality_checks


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

    print(f"Lendo dados brutos de: {input_path}")
    # Lê todos os arquivos .parquet do diretório de entrada
    raw_df = spark.read.parquet(input_path)

    # --- Camada Bronze ---
    print(f"Iniciando escrita na camada Bronze em: {bronze_table_path}")
    raw_df.write.format("delta").mode("overwrite").save(bronze_table_path)

    bronze_table = DeltaTable.forPath(spark, bronze_table_path)
    bronze_records_count = bronze_table.history(1).select("operationMetrics.numOutputRows").collect()[0][0]
    print(f"{bronze_records_count} registros escritos na camada Bronze.")
    spark.sql(f"CREATE TABLE IF NOT EXISTS taxi_bronze USING DELTA LOCATION '{bronze_table_path}'")
    print("Tabela 'taxi_bronze' criada/atualizada.")

    # --- Camada Silver ---
    bronze_df = spark.read.format("delta").load(bronze_table_path)

    transformed_df = bronze_df.select(
        col("VendorID").alias("vendor_id").cast(IntegerType()),
        to_timestamp(col("tpep_pickup_datetime")).alias("pickup_datetime"),
        to_timestamp(col("tpep_dropoff_datetime")).alias("dropoff_datetime"),
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