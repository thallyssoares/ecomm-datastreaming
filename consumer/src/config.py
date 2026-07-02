from pydantic import Field
from pydantic_settings import BaseSettings


class ConsumerConfig(BaseSettings):
    # Kafka
    kafka_bootstrap_servers: str = Field(
        default="localhost:9092", alias="KAFKA_BOOTSTRAP_SERVERS"
    )
    kafka_topic: str = Field(default="ecommerce-events", alias="KAFKA_TOPIC")
    kafka_consumer_group: str = Field(
        default="bytewax-consumer", alias="KAFKA_CONSUMER_GROUP"
    )

    # BigQuery
    gcp_project_id: str = Field(alias="GCP_PROJECT_ID")
    bq_dataset: str = Field(default="bronze", alias="BQ_DATASET_DATASET")
    bq_table: str = Field(default="raw_clickstream", alias="BQ_TABLE")
    bq_location: str = Field(default="US", alias="BQ_LOCATION")

    # Batch processing
    batch_size: int = Field(default=100, alias="BATCH_SIZE")
    batch_timeout_seconds: int = Field(default=10, alias="BATCH_TIMEOUT_SECONDS")

    # Bytewax
    worker_count: int = Field(default=1, alias="WORKER_COUNT")
    worker_id: int = Field(default=0, alias="WORKER_ID")

    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"
        extra = "ignore"


config = Config()
