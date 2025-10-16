from src.utils.data_loader import DataLoader


from src.utils.data_loader import DataLoader


class ExtractData:
    def __init__(self, base_url, file_pattern, year, months, data_lake_path, landing_zone_path):
        self.base_url = base_url
        self.file_pattern = file_pattern
        self.year = year
        self.months = months
        self.data_lake_path = data_lake_path
        self.landing_zone_path = f"{self.data_lake_path}{landing_zone_path}"

    def run(self):
        """Executa o job de extração de dados."""
        self._download_data()

    def _download_data(self):
        """Baixa os dados brutos para a landing zone."""
        print("Iniciando etapa de download dos dados...")
        loader = DataLoader(
            base_url=self.base_url,
            file_pattern=self.file_pattern,
            year=self.year,
            months=self.months,
            landing_zone_path=self.landing_zone_path
        )
        loader.download()