from pyspark.sql import SparkSession
from pyspark.sql.functions import col, month, year, concat_ws
from pyspark.sql.types import IntegerType, DoubleType
from delta.tables import DeltaTable

from src.utils.data_quality import apply_data_quality_checks


def run_silver_ingestion_job(spark: SparkSession, data_lake_path: str):
    """
    Executa o pipeline de processamento da camada Bronze para a Silver.

    Este pipeline lê dados da camada Bronze, aplica transformações,
    realiza checagens de qualidade e armazena os dados limpos na camada Silver
    e os dados reprovados na camada de Quarentena.

    :param spark: A sessão Spark ativa.
    :param data_lake_path: O caminho base onde as tabelas Delta estão localizadas.
    """
    bronze_table_path = f"{data_lake_path}/bronze/yellow_taxi_trips"
    silver_table_path = f"{data_lake_path}/silver/yellow_taxi_trips"
    quarantine_table_path = f"{data_lake_path}/quarantine/yellow_taxi_trips"

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
             .option("delta.feature.timestampNtz", "supported") \
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
            .write.format("delta") \
            .mode("overwrite") \
            .option("delta.feature.timestampNtz", "supported") \
            .save(quarantine_table_path)
        spark.sql(f"CREATE TABLE IF NOT EXISTS taxi_quarantine USING DELTA LOCATION '{quarantine_table_path}'")
        print("Tabela 'taxi_quarantine' criada/atualizada.")

    dq_checked_df.unpersist()

    print("Pipeline de ingestão para a camada Silver concluído com sucesso!")
