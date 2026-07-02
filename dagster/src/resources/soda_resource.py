import os

from pydantic import Field
from soda.scan import Scan

from dagster import ConfigurableResource


class SodaResource(ConfigurableResource):
    """Resource para rodar Soda Checks no Dagster"""

    project_id: str = Field(description="GCP Project ID")
    dataset: str = Field(description="BigQuery dataset para checks")
    checks_path: str = Field(
        default="soda/checks", description="Caminho para YAMLs de checks"
    )

    def scan(self, scan_name: str, checks_subpath: str = "") -> int:
        """
        Executa um scan Soda.
        Retorna código de saída (0 = sucesso).
        """
        scan = Scan()

        # Configuração BigQuery
        scan.set_data_source_name("bigquery")
        scan.add_configuration_yaml_str(f"""
            data_source bigquery:
                type: bigquery
                project_id: {self.project_id}
                dataset: {self.dataset}
        """)

        # Adiciona checks
        checks_dir = os.path.join(self.checks_path, checks_subpath)
        if os.path.exists(checks_dir):
            scan.add_sodacl_yaml_files(checks_dir)
        else:
            raise FileNotFoundError(f"Checks path not found: {checks_dir}")

        # Executa
        result = scan.execute()
        return result

    def scan_with_vars(
        self, scan_name: str, checks_subpath: str, variables: dict
    ) -> int:
        """Executa scan com variáveis (ex: run_date)"""
        scan = Scan()

        scan.set_data_source_name("bigquery")
        scan.add_configuration_yaml_str(f"""
            data_source bigquery:
                type: bigquery
                project_id: {self.project_id}
                dataset: {self.dataset}
        """)

        # Adiciona variáveis
        for key, value in variables.items():
            scan.add_variable(key, value)

        checks_dir = os.path.join(self.checks_path, checks_subpath)
        scan.add_sodacl_yaml_files(checks_dir)

        return scan.execute()


soda_resource = SodaResource(
    project_id="CHANGE_ME",
    dataset="silver",
    checks_path="../soda/checks",  # Relativo ao dagster/
)
