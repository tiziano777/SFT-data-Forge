import logging
from pathlib import Path
from typing import Callable

import pyarrow as pa
import pyarrow.parquet as pq

from .writer import BaseCustomWriter

logger = logging.getLogger(__name__)


class CustomParquetWriter(BaseCustomWriter):
    """
    Writer che salva i documenti in formato Parquet:
        {target_path}/{distribution_relative}/{lang}/{base_filename}_processed_{rank:05d}.parquet

    Usa PyArrow con scrittura a batch: i documenti vengono accumulati in memoria
    per file, poi scritti ogni batch_size documenti o alla fine della pipeline.
    """

    FILE_EXTENSION = ".parquet"

    def __init__(
        self,
        target_path: str,
        distribution_relative: str = "",
        compression: str | None = "snappy",
        batch_size: int = 1000,
        adapter: Callable = None,
        expand_metadata: bool = False,
        max_file_size: int = -1,
    ):
        super().__init__(target_path=target_path, distribution_relative=distribution_relative)
        self.compression = compression
        self.batch_size = batch_size
        self._batches: dict[str, list[dict]] = {}
        self._pq_writers: dict[str, pq.ParquetWriter] = {}

    def _open_file_handler(self, file_path: Path):
        """Ritorna il path come stringa. PyArrow gestisce il file internamente."""
        return str(file_path)

    def _write_to_handle(self, handle: str, transformed: dict):
        """Accumula documenti in un batch. Flush al raggiungimento di batch_size."""
        if handle not in self._batches:
            self._batches[handle] = []

        self._batches[handle].append(transformed)

        if len(self._batches[handle]) >= self.batch_size:
            self._flush_batch(handle)

    def _flush_batch(self, handle_key: str):
        """Scrive il batch accumulato su disco tramite PyArrow."""
        batch_data = self._batches.get(handle_key)
        if not batch_data:
            return

        record_batch = pa.RecordBatch.from_pylist(batch_data)

        if handle_key not in self._pq_writers:
            self._pq_writers[handle_key] = pq.ParquetWriter(
                handle_key,
                schema=record_batch.schema,
                compression=self.compression,
            )

        self._pq_writers[handle_key].write_batch(record_batch)
        self._batches[handle_key] = []

    def _close_handles(self):
        """Flush batch rimanenti e chiude tutti i ParquetWriter."""
        for handle_key in list(self._batches.keys()):
            self._flush_batch(handle_key)

        for handle_key, writer in self._pq_writers.items():
            try:
                writer.close()
                logger.info(f"Chiuso parquet writer: {handle_key}")
            except Exception as e:
                logger.warning(f"Errore chiusura parquet writer {handle_key}: {e}")

        self._pq_writers.clear()
        self._batches.clear()
        self._file_handles.clear()
