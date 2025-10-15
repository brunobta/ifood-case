import os
import requests
import time
import tempfile


def get_dbutils():
    """
    Obtém a instância do dbutils se estiver em um ambiente Databricks.
    Retorna None caso contrário, permitindo execução local para testes.
    """
    try:
        from pyspark.dbutils import DBUtils
        from pyspark.sql import SparkSession
        return DBUtils(SparkSession.builder.getOrCreate())
    except (ImportError, ModuleNotFoundError):
        return None

def download_data(base_url: str, file_pattern: str, years: list, months: list, landing_zone_path: str, retries: int = 3, delay: int = 5):
    """
    Baixa os arquivos de dados de táxi do site da NYC TLC.

    :param base_url: A URL base para download dos arquivos.
    :param file_pattern: O padrão do nome do arquivo com placeholders para ano e mês.
    :param years: Uma lista de anos para baixar.
    :param months: Uma lista de meses para baixar.
    :param landing_zone_path: O caminho para salvar os arquivos.
    :param retries: Número de tentativas de download em caso de falha.
    :param delay: Atraso em segundos entre as tentativas.
    """
    dbutils = get_dbutils()
    if not dbutils:
        raise RuntimeError("Esta função requer um ambiente Databricks para interagir com o S3.")

    # Garante que o diretório de destino no S3 (ou DBFS) exista
    dbutils.fs.mkdirs(landing_zone_path)

    successful_downloads = 0
    total_files = len(years) * len(months)

    for year in years:
        for month in months:
            file_name = file_pattern.format(year=year, month=f"{month:02d}")
            file_url = f"{base_url}/{file_name}"
            destination_path = f"{landing_zone_path.rstrip('/')}/{file_name}"

            try:
                # Usa dbutils para verificar se o arquivo já existe no destino (S3)
                dbutils.fs.ls(destination_path)
                print(f"Arquivo {file_name} já existe. Pulando o download.")
                successful_downloads += 1
                continue
            except Exception:
                # O arquivo não existe, prossegue para o download
                pass

            for attempt in range(retries):
                print(f"Baixando {file_name} de {file_url} (tentativa {attempt + 1}/{retries})...")
                try:
                    response = requests.get(file_url, stream=True, timeout=60)
                    response.raise_for_status()  # Lança um erro para códigos de status ruins (4xx ou 5xx)
                    # Baixa para um arquivo temporário local e depois move para o S3
                    with tempfile.NamedTemporaryFile(delete=False) as tmp_file:
                        for chunk in response.iter_content(chunk_size=8192):
                            tmp_file.write(chunk)
                        # Move o arquivo do disco local do cluster para o destino final no S3
                        dbutils.fs.mv(f"file:{tmp_file.name}", destination_path)
                    print(f"Arquivo {file_name} salvo em {destination_path}")
                    successful_downloads += 1
                    break  # Sucesso, sai do loop de tentativas
                except requests.exceptions.RequestException as e:
                    print(f"Falha na tentativa {attempt + 1} para baixar {file_name}. Erro: {e}")
                    if attempt < retries - 1:
                        time.sleep(delay)
                    else:
                        print(f"Falha ao baixar {file_name} após {retries} tentativas. Pulando para o próximo arquivo.")

    if successful_downloads == 0 and total_files > 0:
        print("\nNenhum arquivo foi baixado com sucesso. Verifique a conexão de rede e as URLs.")
        print("Abortando a execução do pipeline.")
        raise RuntimeError("A etapa de download de dados falhou. Nenhum arquivo foi obtido.")