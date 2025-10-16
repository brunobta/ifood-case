from src.utils.spark import create_spark_session
from src.jobs.bronze_ingestion import BronzeIngestion
from src.jobs.silver_ingestion import SilverIngestion
from src.jobs.extract_data import ExtractData
from src.config import load_config_as_env

import sys
import os

def main():
    """Executa o pipeline completo."""
    if len(sys.argv) < 2:
        print("Erro: Nenhum parâmetro foi passado.")
        sys.exit(1)

    task_name = sys.argv[1]

    match task_name:
        case "Extract":
            """Executa o pipeline de extração de dados."""
            if len(sys.argv) != 5:
                print("Uso: python main.py Extract <ano> <mes_inicio> <mes_fim>")
                sys.exit(1)
            
            ano = sys.argv[2]
            try:
                mes_inicio = int(sys.argv[3])
                mes_fim = int(sys.argv[4])
            except ValueError:
                print("Erro: Os meses de início e fim devem ser números inteiros.")
                sys.exit(1)

            meses = list(range(mes_inicio, mes_fim + 1))

            print("\nIniciando etapa de extração de dados...")
            job = ExtractData(
                base_url=os.getenv("base_url"),
                file_pattern=os.getenv("source") + "_{year}-{month}.parquet",
                year=ano,
                months=meses,
                data_lake_path=os.getenv("data_lake_path"),
                landing_zone_path=os.getenv("landing_zone_path")
            )
            job.run()
            print("\nExtração concluída.")
        case "Bronze":
            """Executa o pipeline de ingestão para a camada Bronze."""
            print("\nIniciando etapa de ingestão para a camada Bronze...")
            job = BronzeIngestion(
                spark=create_spark_session('Bronze Ingestion'),
                input_path=os.getenv("data_lake_path") + os.getenv("landing_zone_path"),
                data_lake_path=os.getenv("data_lake_path"),
                bronze_table_path=os.getenv("bronze_table_path")
            )
            job.run()
            print("\nIngestão para a camada Bronze concluída.")
        case "Silver":
            """Executa o pipeline de processamento da camada Bronze para a Silver."""
            print("\nIniciando etapa de processamento para a camada Silver...")
            job = SilverIngestion(
                spark=create_spark_session('Silver Ingestion'),
                data_lake_path=os.getenv("data_lake_path")
            )
            job.run()
            print("\nProcessamento para a camada Silver concluído.")
        case _:
            print(f"Tarefa desconhecida: {task_name}")
            sys.exit(1)

if __name__ == "__main__":
    load_config_as_env()
    main()