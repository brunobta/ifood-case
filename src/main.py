from src.common.spark import create_spark_session
from src.jobs.taxi_ingestion import run_ingestion_job
from src.utils.data_loader import download_data


def main():
    """Ponto de entrada principal para orquestrar o pipeline de dados."""
    spark = create_spark_session("TaxiDataIngestion")

    # --- Configuração dos Parâmetros ---
    # Parâmetros para o download dos dados
    base_url = "https://d37ci6vzurychx.cloudfront.net/trip-data"
    file_pattern = "yellow_tripdata_{year}-{month}.parquet"
    years_to_download = [2023]
    months_to_download = range(1, 6)  # Janeiro a Maio

    # Caminhos para o Data Lake no Databricks (DBFS)
    # O Spark requer o prefixo 'dbfs:/' para acessar o Databricks File System.
    landing_zone_path = "dbfs:/FileStore/ifood_case/landing_zone/"
    data_lake_path = "dbfs:/FileStore/ifood_case/data_lake"

    # --- Execução do Pipeline ---
    # 1. Baixar os dados brutos para a landing zone
    print("Iniciando etapa de download dos dados...")
    download_data(base_url, file_pattern, years_to_download, months_to_download, landing_zone_path)

    # 2. Executar o pipeline de ingestão e transformação (Bronze -> Silver)
    print("\nIniciando etapa de ingestão e transformação...")
    run_ingestion_job(spark, landing_zone_path, data_lake_path)


if __name__ == "__main__":
    main()