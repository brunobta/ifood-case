from pyspark.sql import SparkSession
from pyspark.dbutils import DBUtils
from functools import reduce
from delta.tables import DeltaTable


class BronzeIngestionJob:
    def __init__(self, spark: SparkSession, input_path: str, data_lake_path: str):
        self.spark = spark
        self.input_path = input_path
        self.data_lake_path = data_lake_path
        self.bronze_table_path = f"{self.data_lake_path}/bronze/yellow_taxi_trips"

    def run(self):
        """
        Executa o pipeline de ingestão de dados de táxis de NY para a camada Bronze.
        """
        df = self._read_raw_data()
        self._write_to_bronze(df)

    def _read_raw_data(self):
        """
        Lê dados brutos em formato parquet, padroniza os nomes das colunas e os une.
        """
        print(f"Lendo dados brutos de: {self.input_path}")
        dbutils = DBUtils(self.spark)
        files = dbutils.fs.ls(self.input_path)
        parquet_files = [f.path for f in files if f.path.endswith(".parquet")]

        all_dfs = []
        for file_path in parquet_files:
            print(f"Processando arquivo: {file_path}")
            df = self.spark.read.parquet(file_path)
            # Padroniza os nomes das colunas para snake_case para este arquivo
            for column in df.columns:
                new_column_name = column.lower().replace(' ', '_')
                df = df.withColumnRenamed(column, new_column_name)
            all_dfs.append(df)

        if not all_dfs:
            raise ValueError(f"Nenhum arquivo parquet encontrado em {self.input_path}")

        # Une todos os dataframes usando unionByName para lidar com ordens de colunas diferentes
        return reduce(lambda df1, df2: df1.unionByName(df2), all_dfs)

    def _write_to_bronze(self, df):
        """
        Escreve o DataFrame na camada Bronze em formato Delta.
        """
        print(f"Iniciando escrita na camada Bronze em: {self.bronze_table_path}")
        df.write.format("delta") \
            .mode("overwrite") \
            .option("overwriteSchema", "true") \
            .save(self.bronze_table_path)

        bronze_table = DeltaTable.forPath(self.spark, self.bronze_table_path)
        bronze_records_count = bronze_table.history(1).select("operationMetrics.numOutputRows").collect()[0][0]
        print(f"{bronze_records_count} registros escritos na camada Bronze.")
        self.spark.sql(f"CREATE TABLE IF NOT EXISTS taxi_bronze USING DELTA LOCATION '{self.bronze_table_path}'")
        print("Tabela 'taxi_bronze' criada/atualizada.")