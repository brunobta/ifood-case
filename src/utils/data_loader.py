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

class DataLoader:
    def __init__(self, base_url: str, file_pattern: str, years: list, months: list, landing_zone_path: str, retries: int = 3, delay: int = 5):
        self.base_url = base_url
        self.file_pattern = file_pattern
        self.years = years
        self.months = months
        self.landing_zone_path = landing_zone_path
        self.retries = retries
        self.delay = delay
        self.dbutils = get_dbutils()
        if not self.dbutils:
            raise RuntimeError("Esta função requer um ambiente Databricks para interagir com o S3.")

    def download(self):
        """
        Baixa os arquivos de dados de táxi do site da NYC TLC.
        """
        self.dbutils.fs.mkdirs(self.landing_zone_path)

        successful_downloads = 0
        total_files = len(self.years) * len(self.months)

        for year in self.years:
            for month in self.months:
                file_name = self.file_pattern.format(year=year, month=f"{month:02d}")
                file_url = f"{self.base_url}/{file_name}"
                destination_path = f"{self.landing_zone_path.rstrip('/')}/{file_name}"

                if self._file_exists(destination_path):
                    print(f"Arquivo {file_name} já existe. Pulando o download.")
                    successful_downloads += 1
                    continue

                if self._download_file(file_name, file_url, destination_path):
                    successful_downloads += 1

        if successful_downloads == 0 and total_files > 0:
            print("\nNenhum arquivo foi baixado com sucesso. Verifique a conexão de rede e as URLs.")
            print("Abortando a execução do pipeline.")
            raise RuntimeError("A etapa de download de dados falhou. Nenhum arquivo foi obtido.")

    def _file_exists(self, path: str) -> bool:
        """
        Verifica se um arquivo existe no caminho de destino.
        """
        try:
            self.dbutils.fs.ls(path)
            return True
        except Exception:
            return False

    def _download_file(self, file_name: str, file_url: str, destination_path: str) -> bool:
        """
        Baixa um único arquivo.
        """
        for attempt in range(self.retries):
            print(f"Baixando {file_name} de {file_url} (tentativa {attempt + 1}/{self.retries})...")
            try:
                print("Iniciando download...")
                response = requests.get(file_url, stream=True, timeout=60)
                response.raise_for_status()
                
                temp_local_path = os.path.join(os.getcwd(), f"temp_{file_name}")
                
                try:
                    with open(temp_local_path, "wb") as f:
                        for chunk in response.iter_content(chunk_size=8192):
                            f.write(chunk)
                    
                    self.dbutils.fs.cp(f"file:{temp_local_path}", destination_path)
                finally:
                    if os.path.exists(temp_local_path):
                        os.remove(temp_local_path)

                print(f"Arquivo {file_name} salvo em {destination_path}")
                return True
            except requests.exceptions.RequestException as e:
                print(f"Falha na tentativa {attempt + 1} para baixar {file_name}. Erro: {e}")
                if attempt < self.retries - 1:
                    time.sleep(self.delay)
                else:
                    print(f"Falha ao baixar {file_name} após {self.retries} tentativas. Pulando para o próximo arquivo.")
                    return False
        return False

