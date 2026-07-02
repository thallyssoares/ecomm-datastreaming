#!/usr/bin/env python3
"""
Entry point para o consumer Bytewax.

Roda o dataflow com configuração de workers.
"""

import logging
import sys

from bytewax.run import run_main
from bytewax_flow import build_flow
from config import config

# Configura logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
    datefmt="%H:%M:%S",
)

# Reduz verbosidade de libs
logging.getLogger("kafka").setLevel(logging.WARNING)
logging.getLogger("google").setLevel(logging.WARNING)
logging.getLogger("bytewax").setLevel(logging.INFO)

logger = logging.getLogger(__name__)


def main():
    logger.info("=" * 60)
    logger.info("🚀 Iniciando Bytewax Consumer - E-commerce Clickstream")
    logger.info("=" * 60)
    logger.info(f"Kafka: {config.kafka_bootstrap_servers}")
    logger.info(f"Topic: {config.kafka_topic}")
    logger.info(f"Consumer Group: {config.kafka_consumer_group}")
    logger.info(
        f"BigQuery: {config.gcp_project_id}.{config.bq_dataset}.{config.bq_table}"
    )
    logger.info(f"Workers: {config.worker_count}")
    logger.info("=" * 60)

    # Valida configuração obrigatória
    if not config.gcp_project_id:
        logger.error("GCP_PROJECT_ID não configurado!")
        sys.exit(1)

    # Build e roda flow
    flow = build_flow()

    # run_main bloqueia até shutdown
    run_main(
        flow,
        # Configurações de execução
        worker_count=config.worker_count,
        worker_id=config.worker_id,
    )


if __name__ == "__main__":
    main()
