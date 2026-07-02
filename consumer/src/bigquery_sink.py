"""
BigQuery Sink para Bytewax - Escrita em batch com retry e métricas.

Usa google-cloud-bigquery com insert_rows_json para streaming insert.
"""

import json
import logging
import time
from datetime import datetime
from typing import Any, List, Optional

from config import config
from google.api_core import retry
from google.cloud import bigquery
from google.cloud.exceptions import GoogleCloudError

logger = logging.getLogger(__name__)


class BigQuerySink:
    """Sink BigQuery otimizado para streaming inserts"""

    def __init__(self):
        self.client = bigquery.Client(
            project=config.gcp_project_id,
            location=config.bq_location,
        )
        self.table_id = f"{config.gcp_project_id}.{config.bq_dataset}.{config.bq_table}"
        self.table = self.client.get_table(self.table_id)
        self.batch_size = config.batch_size
        self.batch_timeout = config.batch_timeout_seconds

        # Buffer para batch
        self._buffer: List[dict] = []
        self._last_flush = time.time()

        # Métricas
        self.rows_inserted = 0
        self.rows_failed = 0
        self.batches_sent = 0
        self.last_error: Optional[str] = None

    def write(self, events: List[dict]) -> int:
        """
        Escreve lista de eventos no BigQuery.
        Retorna número de linhas inseridas com sucesso.
        """
        if not events:
            return 0

        # Converte para formato BigQuery
        rows = [self._prepare_row(event) for event in events]

        # Insert com retry
        errors = self._insert_with_retry(rows)

        success_count = len(rows) - len(errors)
        self.rows_inserted += success_count
        self.rows_failed += len(errors)
        self.batches_sent += 1

        if errors:
            self.last_error = str(errors[0])
            logger.error(f"BigQuery insert errors: {errors[:3]}")
        else:
            self.last_error = None

        logger.debug(f"Inseridos {success_count}/{len(rows)} linhas no BigQuery")
        return success_count

    def _prepare_row(self, event: dict) -> dict:
        """Prepara evento para schema BigQuery (converte tipos)"""
        row = event.copy()

        # Garante timestamps como string ISO (BigQuery aceita)
        for ts_field in ["event_timestamp", "processing_timestamp"]:
            if ts_field in row and row[ts_field]:
                if isinstance(row[ts_field], datetime):
                    row[ts_field] = row[ts_field].isoformat()
                elif not isinstance(row[ts_field], str):
                    row[ts_field] = str(row[ts_field])

        # Converte float para string se necessário (evita precision issues)
        if "price" in row and row["price"] is not None:
            row["price"] = float(row["price"])

        return row

    @retry.Retry(
        predicate=retry.if_transient_error,
        initial=1.0,
        maximum=30.0,
        multiplier=2.0,
        deadline=60.0,
    )
    def _insert_with_retry(self, rows: List[dict]) -> List[dict]:
        """Insert com retry automático para erros transientes"""
        errors = self.client.insert_rows_json(self.table, rows)
        return errors

    def flush(self) -> int:
        """Força flush do buffer (para graceful shutdown)"""
        if not self._buffer:
            return 0

        rows = self._buffer.copy()
        self._buffer.clear()
        self._last_flush = time.time()

        return self.write(rows)

    def add_to_buffer(self, event: dict):
        """Adiciona evento ao buffer para batch"""
        self._buffer.append(event)

        # Flush se atingiu tamanho ou timeout
        if (
            len(self._buffer) >= self.batch_size
            or time.time() - self._last_flush >= self.batch_timeout
        ):
            self.flush()

    def get_stats(self) -> dict:
        return {
            "rows_inserted": self.rows_inserted,
            "rows_failed": self.rows_failed,
            "batches_sent": self.batches_sent,
            "buffer_size": len(self._buffer),
            "last_error": self.last_error,
            "success_rate": self.rows_inserted
            / max(self.rows_inserted + self.rows_failed, 1),
        }

    def close(self):
        """Flush final e limpeza"""
        self.flush()
        self.client.close()


# Bytewax sink function (stateless, chamado para cada batch)
def bigquery_sink_fn(events: List[dict]) -> None:
    """Função sink compatível com bytewax operators.output"""
    # Cria sink por worker (evita compartilhar client entre threads)
    sink = BigQuerySink()
    try:
        sink.write(events)
    finally:
        sink.close()
