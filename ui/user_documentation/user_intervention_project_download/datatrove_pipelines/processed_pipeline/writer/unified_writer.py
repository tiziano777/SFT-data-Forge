import logging
from typing import Callable

from datatrove.data import DocumentsPipeline

from .writer import CustomJsonlWriter
from .parquet_writer import CustomParquetWriter

logger = logging.getLogger(__name__)


class UnifiedWriter:
    """
    Writer unificato che supporta multipli formati di output,
    seguendo il pattern di UnifiedReader.
    Delega tutto il lavoro al writer specifico per formato.

    Formati supportati: "jsonl.gz" (default), "parquet"
    """

    WRITER_MAPPING = {
        "jsonl.gz": CustomJsonlWriter,
        "parquet": CustomParquetWriter,
    }

    FORMAT_METADATA = {
        "jsonl.gz": {"glob": "*.jsonl.gz", "format": "jsonl.gz"},
        "parquet":  {"glob": "*.parquet",  "format": "parquet"},
    }

    def __init__(
        self,
        output_format: str,
        target_path: str,
        distribution_relative: str = "",
        compression: str | None = None,
        batch_size: int = 1000,
        adapter: Callable = None,
        expand_metadata: bool = False,
        max_file_size: int = -1,
    ):
        self.output_format = output_format.lower().strip()

        if self.output_format not in self.WRITER_MAPPING:
            raise ValueError(
                f"Formato di output non supportato: '{self.output_format}'. "
                f"Supportati: {list(self.WRITER_MAPPING.keys())}"
            )

        writer_class = self.WRITER_MAPPING[self.output_format]

        writer_kwargs = {
            "target_path": target_path,
            "distribution_relative": distribution_relative,
        }

        if self.output_format == "parquet":
            writer_kwargs["compression"] = compression or "snappy"
            writer_kwargs["batch_size"] = batch_size
        elif self.output_format == "jsonl.gz":
            writer_kwargs["compression"] = compression or "gzip"

        logger.info(f"Creazione writer {self.output_format} per {target_path}")
        self.writer = writer_class(**writer_kwargs)

    def get_format_metadata(self) -> dict:
        """Ritorna {'glob': '*.parquet', 'format': 'parquet'} per il formato selezionato."""
        return self.FORMAT_METADATA[self.output_format]

    # --- Delegazione interfaccia PipelineStep ---

    def __call__(self, data: DocumentsPipeline, rank: int = 0, world_size: int = 1):
        return self.writer(data, rank, world_size)

    def run(self, data: DocumentsPipeline, rank: int = 0, world_size: int = 1):
        return self.writer.run(data, rank, world_size)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        return self.writer.__exit__(exc_type, exc_val, exc_tb)
