"""
Bytewax Dataflow para consumir Kafka e escrever no BigQuery.

Flow: Kafka Source -> Parse JSON -> Add processing_timestamp -> BigQuery Sink
"""

import json
import logging
from datetime import datetime, timezone

import bytewax.operators as op
from bigquery_sink import bigquery_sink_fn
from bytewax.connectors.kafka import KafkaSourceMessage, kafka_source
from bytewax.dataflow import Dataflow
from config import config

logger = logging.getLogger(__name__)


def parse_kafka_message(msg: KafkaSourceMessage) -> dict:
    """
    Parse mensagem Kafka -> dict do evento.

    Kafka message structure:
    - key: bytes (user_id para partitioning)
    - value: bytes (JSON do evento)
    - timestamp: tuple (type, ms) - Kafka timestamp
    """
    try:
        # Decode value
        event = json.loads(msg.value.decode("utf-8"))

        # Adiciona metadata de processamento
        event["processing_timestamp"] = datetime.now(timezone.utc).isoformat()

        # Se não tem event_id, gera um
        if "event_id" not in event:
            event["event_id"] = f"kafka-{msg.topic}-{msg.partition}-{msg.offset}"

        # Garante campos obrigatórios
        required = ["user_id", "event_type", "event_timestamp"]
        for field in required:
            if field not in event:
                raise ValueError(f"Campo obrigatório ausente: {field}")

        return event

    except json.JSONDecodeError as e:
        logger.error(f"Erro ao parsear JSON: {e}, value: {msg.value[:100]}")
        raise
    except Exception as e:
        logger.error(f"Erro ao processar mensagem: {e}")
        raise


def validate_event(event: dict) -> bool:
    """Validação básica do evento antes de enviar pro BigQuery"""
    # Tipos válidos
    valid_types = {"page_view", "view_item", "add_to_cart", "purchase"}

    if event.get("event_type") not in valid_types:
        logger.warning(f"event_type inválido: {event.get('event_type')}")
        return False

    # user_id não pode ser vazio
    if not event.get("user_id"):
        logger.warning("user_id vazio")
        return False

    return True


def build_flow() -> Dataflow:
    """Constrói o dataflow Bytewax"""
    flow = Dataflow("ecommerce-clickstream-consumer")

    # 1. Source: Kafka
    kafka_stream = op.input(
        "kafka-input",
        flow,
        kafka_source(
            bootstrap_servers=config.kafka_bootstrap_servers,
            topics=[config.kafka_topic],
            group_id=config.kafka_consumer_group,
            # Configurações de performance
            auto_offset_reset="latest",
            enable_auto_commit=True,
            auto_commit_interval_ms=5000,
            # Exactly-once: desabilitado para simplicidade (idempotência via event_id na Silver)
            isolation_level="read_uncommitted",
        ),
    )

    # 2. Parse JSON
    parsed = op.map("parse-json", kafka_stream, parse_kafka_message)

    # 3. Filtra eventos inválidos (side output para dead letter seria ideal)
    validated = op.filter("validate-event", parsed, validate_event)

    # 4. Log para debug (sample)
    logged = op.inspect(
        "log-sample",
        validated,
        lambda e: logger.debug(f"Evento: {e['event_type']} user={e['user_id']}"),
    )

    # 5. Sink: BigQuery (batch por worker)
    op.output("bigquery-sink", logged, bigquery_sink_fn)

    return flow
