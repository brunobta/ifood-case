from src.common.spark import create_spark_session
from src.jobs.bronze_ingestion import BronzeIngestionJob
from src.jobs.silver_ingestion import SilverIngestionJob
from src.utils.data_loader import DataLoader


class TaxiPipeline:
    def __init__(self, spark):
        self.spark = spark
        self.base_url = "https://d37ci6vzurychx.cloudfront.net/trip-data"
        self.file_pattern = "yellow_tripdata_{year}-{month}.parquet"
        self.years_to_download = [2023]
        self.months_to_download = range(1, 6)  # Janeiro a Maio
        self.landing_zone_path = "s3://datalake-ifood-case/landing/"
        self.data_lake_path = "s3://datalake-ifood-case"

    def run(self):
        """Executa o pipeline completo."""
        self._download_data()
        self._run_bronze_ingestion()
        self._run_silver_ingestion()

    def _download_data(self):
        """Baixa os dados brutos para a landing zone."""
        print("Iniciando etapa de download dos dados...")
        loader = DataLoader(
            base_url=self.base_url,
            file_pattern=self.file_pattern,
            years=self.years_to_download,
            months=self.months_to_download,
            landing_zone_path=self.landing_zone_path,
        )
        loader.download()

    def _run_bronze_ingestion(self):
        """Executa o pipeline de ingestão para a camada Bronze."""
        print("\nIniciando etapa de ingestão para a camada Bronze...")
        job = BronzeIngestionJob(
            self.spark, self.landing_zone_path, self.data_lake_path
        )
        job.run()

    def _run_silver_ingestion(self):
        """Executa o pipeline de processamento da camada Bronze para a Silver."""
        print("\nIniciando etapa de processamento para a camada Silver...")
        job = SilverIngestionJob(self.spark, self.data_lake_path)
        job.run()


def main():
    """Ponto de entrada principal para orquestrar o pipeline de dados."""
    spark = create_spark_session("TaxiDataIngestion")
    pipeline = TaxiPipeline(spark)
    pipeline.run()


if __name__ == "__main__":
    main()
