"""
Dagster Resources - Configuração de conexões externas
"""

from google.cloud import bigquery
from pydantic import Field

from dagster import ConfigurableResource


class BigQueryResource(ConfigurableResource):
    """Resource para Google BigQuery"""

    project_id: str = Field(description="GCP Project ID")
    location: str = Field(default="US", description="BigQuery location")

    def get_client(self) -> bigquery.Client:
        return bigquery.Client(project=self.project_id, location=self.location)

    @property
    def client(self) -> bigquery.Client:
        return self.get_client()


bigquery_resource = BigQueryResource(
    project_id="{{ env_var('GCP_PROJECT_ID') }}",
    location="{{ env_var('BQ_LOCATION', 'US') }}",
)
