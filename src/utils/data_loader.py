import os
import requests
import time


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
    for year in years:
        for month in months:
            file_name = file_pattern.format(year=year, month=f"{month:02d}")
            file_url = f"{base_url}/{file_name}"
            local_file_path = os.path.join(landing_zone_path, file_name)

            if os.path.exists(local_file_path):
                print(f"Arquivo {file_name} já existe. Pulando o download.")
                continue

            for attempt in range(retries):
                print(f"Baixando {file_name} de {file_url} (tentativa {attempt + 1}/{retries})...")
                try:
                    response = requests.get(file_url, stream=True, timeout=60)
                    response.raise_for_status()  # Lança um erro para códigos de status ruins (4xx ou 5xx)
                    with open(local_file_path, "wb") as f:
                        for chunk in response.iter_content(chunk_size=8192):
                            f.write(chunk)
                    print(f"Arquivo {file_name} salvo em {local_file_path}")
                    break  # Sucesso, sai do loop de tentativas
                except requests.exceptions.RequestException as e:
                    print(f"Falha na tentativa {attempt + 1} para baixar {file_name}. Erro: {e}")
                    if attempt < retries - 1:
                        time.sleep(delay)
                    else:
                        print(f"Falha ao baixar {file_name} após {retries} tentativas. Pulando para o próximo.")