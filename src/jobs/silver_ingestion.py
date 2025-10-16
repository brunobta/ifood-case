from pyspark.sql import SparkSession
from pyspark.sql.functions import col, month, year, concat_ws
from pyspark.sql.types import IntegerType, DoubleType
from delta.tables import DeltaTable

from src.utils.data_quality import apply_data_quality_checks


class SilverIngestion:
    def __init__(self, spark: SparkSession, data_lake_path: str, bronze_table_path: str , silver_table_path: str, quarantine_table_path: str):
        self.spark = spark
        self.data_lake_path = data_lake_path
        self.bronze_table_path = f"{self.data_lake_path}{bronze_table_path}"
        self.silver_table_path = f"{self.data_lake_path}{silver_table_path}"
        self.quarantine_table_path = f"{self.data_lake_path}{quarantine_table_path}"

    def run(self):
        """
        Executa o pipeline de processamento da camada Bronze para a Silver.
        """
        bronze_df = self._read_bronze_data()
        transformed_df = self._transform_data(bronze_df)
        dq_checked_df = self._apply_data_quality_checks(transformed_df)
        
        dq_checked_df.cache()

        good_records_df = dq_checked_df.filter(col("dq_status") == "PASS").drop("dq_status", "dq_failures")
        quarantined_records_df = dq_checked_df.filter(col("dq_status") == "FAIL")

        self._write_silver_data(good_records_df)
        self._write_quarantine_data(quarantined_records_df)

        dq_checked_df.unpersist()
        print("Pipeline de ingestão para a camada Silver concluído com sucesso!")

    def _read_bronze_data(self):
        """
        Lê os dados da camada Bronze.
        """
        return self.spark.read.format("delta").load(self.bronze_table_path)

    def _transform_data(self, df):
        """
        Aplica transformações nos dados.
        """
        return df.select(
            col("vendorid").alias("vendor_id").cast(IntegerType()),
            col("tpep_pickup_datetime").alias("pickup_datetime"),
            col("tpep_dropoff_datetime").alias("dropoff_datetime"),
            col("passenger_count").cast(IntegerType()),
            col("total_amount").cast(DoubleType())
        )

    def _apply_data_quality_checks(self, df):
        """
        Aplica checagens de qualidade de dados.
        """
        print("Aplicando regras de qualidade de dados...")
        return apply_data_quality_checks(df)

    def _write_silver_data(self, df):
        """
        Escreve os dados na camada Silver.
        """
        silver_df = df.withColumn("pickup_month", month(col("pickup_datetime"))) \
                      .withColumn("pickup_year", year(col("pickup_datetime")))

        print(f"Iniciando escrita na camada Silver em: {self.silver_table_path}")
        silver_df.write.format("delta") \
                 .mode("overwrite") \
                 .partitionBy("pickup_year", "pickup_month") \
                 .save(self.silver_table_path)

        silver_table = DeltaTable.forPath(self.spark, self.silver_table_path)
        silver_records_count = silver_table.history(1).select("operationMetrics.numOutputRows").collect()[0][0]
        print(f"{silver_records_count} registros escritos na camada Silver.")
        self.spark.sql(f"CREATE TABLE IF NOT EXISTS taxi_silver USING DELTA LOCATION '{self.silver_table_path}'")
        print("Tabela 'taxi_silver' criada/atualizada.")

    def _write_quarantine_data(self, df):
        """
        Escreve os dados na camada de Quarentena.
        """
        quarantined_count = df.count()
        print(f"Encontrados {quarantined_count} registros para a quarentena.")
        if quarantined_count > 0:
            print(f"Escrevendo registros na camada de Quarentena em: {self.quarantine_table_path}")
            df.withColumn("dq_failures", concat_ws(",", col("dq_failures"))) \
                .write.format("delta") \
                .mode("overwrite") \
                .save(self.quarantine_table_path)
            self.spark.sql(f"CREATE TABLE IF NOT EXISTS taxi_quarantine USING DELTA LOCATION '{self.quarantine_table_path}'")
            print("Tabela 'taxi_quarantine' criada/atualizada.")
