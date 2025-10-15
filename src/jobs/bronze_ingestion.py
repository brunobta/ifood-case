from pyspark.sql import SparkSession
from pyspark.dbutils import DBUtils
from functools import reduce
from delta.tables import DeltaTable


def run_bronze_ingestion_job(spark: SparkSession, input_path: str, data_lake_path: str):
    """
    Executa o pipeline de ingestão de dados de táxis de NY para a camada Bronze.

    Este pipeline lê dados brutos em formato parquet e os ingere em uma
    camada Bronze (Delta).

    :param spark: A sessão Spark ativa.
    :param input_path: O caminho para os dados brutos (landing zone).
    :param data_lake_path: O caminho base para salvar as tabelas Delta.
    """
    bronze_table_path = f"{data_lake_path}/bronze/yellow_taxi_trips"

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
    # Habilita o suporte a TimestampNTZ, necessário para os dados de origem.
    standardized_df.write.format("delta") \
                   .mode("overwrite") \
                   .option("overwriteSchema", "true") \
                   .save(bronze_table_path)

    bronze_table = DeltaTable.forPath(spark, bronze_table_path)
    bronze_records_count = bronze_table.history(1).select("operationMetrics.numOutputRows").collect()[0][0]
    print(f"{bronze_records_count} registros escritos na camada Bronze.")
    spark.sql(f"CREATE TABLE IF NOT EXISTS taxi_bronze USING DELTA LOCATION '{bronze_table_path}'")
    print("Tabela 'taxi_bronze' criada/atualizada.")
