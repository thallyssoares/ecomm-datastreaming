from google.cloud import bigquery
from pydantic import Field

from dagster import ConfigurableResource


class BigQueryResource(ConfigurableResource):
    """Resource para conexão BigQuery no Dagster"""

    project: str = Field(description="GCP Project ID")
    location: str = Field(default="US", description="BigQuery location")

    def get_client(self) -> bigquery.Client:
        return bigquery.Client(project=self.project, location=self.location)

    @property
    def client(self) -> bigquery.Client:
        return self.get_client()


# Instância padrão (configurada via launchpad ou yaml)
bigquery_resource = BigQueryResource(
    project="CHANGE_ME",  # Será sobrescrito no dagster.yaml
    location="US",
)
