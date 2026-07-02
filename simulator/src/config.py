from pydantic_settings import BaseSettings
from pydantic import Field


class SimulatorConfig(BaseSettings):
    # Kafka
    kafka_bootstrap_servers: str = Field(default="localhost:9092", alias="KAFKA_BOOTSTRAP_SERVERS")
    kafka_topic: str = Field(default="ecommerce-events", alias="KAFKA_TOPIC")

    # Simulation control
    events_per_second: float = Field(default=10.0, alias="EVENTS_PER_SECOND")
    user_pool_size: int = Field(default=1000, alias="USER_POOL_SIZE")
    session_duration_minutes: int = Field(default=30, alias="SESSION_DURATION_MINUTES")

    # Product catalog (static IDs for join with dim_products)
    product_catalog_size: int = Field(default=50, alias="PRODUCT_CATALOG_SIZE")
    category_count: int = Field(default=10, alias="CATEGORY_COUNT")

    # Behavior probabilities (must sum ~1.0 for funnel)
    prob_page_view: float = 0.40
    prob_view_item: float = 0.30
    prob_add_to_cart: float = 0.20
    prob_purchase: float = 0.10

    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"
        extra = "ignore"


config = SimulatorConfig()
