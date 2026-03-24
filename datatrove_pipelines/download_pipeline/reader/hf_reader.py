# reader/hf_reader.py
from typing import Callable
from pathlib import Path
from loguru import logger
import copy

from datatrove.data import Document, DocumentsPipeline
from datatrove.pipeline.readers.huggingface import HuggingFaceDatasetReader


class CustomHuggingFaceReader(HuggingFaceDatasetReader):
    """
    Estensione di HuggingFaceDatasetReader che aggiunge metadati compatibili con CustomJsonlWriter.
    Se non viene specificata una configurazione per un dataset multi-config, 
    itera automaticamente su tutte le configurazioni disponibili creando la ramificazione corretta.
    """

    name = "🤗 Custom HuggingFace"

    def __init__(
        self,
        dataset: str,
        base_output_path: str,
        dataset_options: dict | None = None,
        streaming: bool = False,
        limit: int = -1,
        skip: int = 0,
        batch_size: int = 1000,
        doc_progress: bool = False,
        adapter: Callable = None,
        text_key: str = "text",
        id_key: str = "id",
        default_metadata: dict = None,
        shuffle_files: bool = False,
        distribution_key: str = None,
        available_languages: list = None,
    ):
        self.base_output_path = Path(base_output_path)
        self.dataset_name = dataset
        self.dataset_options_dict = dataset_options or {}
        self.available_languages = available_languages or ["un"]
        self.distribution_key = distribution_key
        
        # Prepariamo i metadati iniziali
        enhanced_default_metadata = {
            "_dataset_name": self.dataset_name,
            "_dataset_path": str(self.base_output_path),
            "_available_languages": self.available_languages,
        }
        
        if default_metadata:
            enhanced_default_metadata.update(default_metadata)
        
        super().__init__(
            dataset=dataset,
            dataset_options=dataset_options,
            streaming=streaming,
            limit=limit,
            skip=skip,
            batch_size=batch_size,
            doc_progress=doc_progress,
            adapter=adapter,
            text_key=text_key,
            id_key=id_key,
            default_metadata=enhanced_default_metadata,
            shuffle_files=shuffle_files,
        )
        self.split = self.dataset_options_dict.get("split", "train")

        logger.info(f"📚 Dataset: {self.dataset}")
        logger.info(f"📁 Output path: {self.base_output_path}")
        logger.info(f"🌍 Split: {self.split}")
        logger.info(f"🗣️ Lingue disponibili: {self.available_languages}")

    def _default_adapter(self, data: dict, path: str, id_in_file: int | str) -> dict:
        import sys
        import os
        # Aggiungi il path del modulo stats_functions
        sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
        from .stats_functions.core_metadata import hf_default_adapter_aux
        
        return hf_default_adapter_aux(data, path, id_in_file, self.default_metadata)

    def get_document_from_dict(self, data: dict, source_file: str, id_in_file: int | str):
        """
        Crea il documento applicando l'adapter e gestendo la chiave distribution.
        """
        if self.adapter:
            document_dict = self.adapter(data, source_file, id_in_file)
        else:
            document_dict = self._default_adapter(data, source_file, id_in_file)
        
        if isinstance(document_dict, dict):
            document = Document(**document_dict)
        else:
            document = document_dict
        
        if not document or not document.text:
            return None
        
        # Assicuriamo che il dataset sorgente sia tracciato
        document.metadata.setdefault("dataset", source_file)
        
        # Se presente distribution_key (es. 'category'), lo salviamo nei metadati
        if self.distribution_key and self.distribution_key in data:
            document.metadata["distribution"] = str(data.get(self.distribution_key, ""))
        
        return document

    def run(self, data: DocumentsPipeline = None, rank: int = 0, world_size: int = 1) -> DocumentsPipeline:
        from datasets import load_dataset, get_dataset_config_names
        from tqdm import tqdm

        if data:
            yield from data

        # 1. Identificazione configurazioni (es. ARC-Challenge, ARC-Easy)
        configs = [None]
        if "name" not in self.dataset_options_dict:
            try:
                available_configs = get_dataset_config_names(self.dataset)
                if available_configs and len(available_configs) > 1:
                    configs = available_configs
                    logger.info(f"🔄 Multi-config dataset detected. Processing: {configs}")
            except Exception as e:
                logger.warning(f"Could not fetch config names for {self.dataset}: {e}")

        # 2. Iterazione su ogni configurazione
        for config in configs:
            current_options = copy.deepcopy(self.dataset_options_dict)
            
            # Determiniamo il nome completo (es. allenai/ai2_arc/ARC-Challenge)
            # Questo valore sovrascrive quello di default per permettere al writer 
            # di creare la sottocartella corretta
            full_ds_name = f"{self.dataset}/{config}" if config else self.dataset
            self.default_metadata["_dataset_name"] = full_ds_name
            
            if config:
                current_options["name"] = config
                logger.info(f"🚀 Loading config: {config}")

            try:
                ds = load_dataset(self.dataset, **current_options, streaming=self.streaming)
            except Exception as e:
                logger.error(f"❌ Failed to load dataset {self.dataset} (config: {config}): {e}")
                continue

            # Gestione Split
            if isinstance(ds, dict):
                if self.split and self.split in ds:
                    ds = ds[self.split]
                else:
                    raise ValueError(
                        f"Split non trovato. Disponibili: {list(ds.keys())}"
                    )

            if self.shuffle_files:
                ds = ds.shuffle(seed=42) if not self.streaming else ds.shuffle(seed=42, buffer_size=1000)

            # 3. Sharding e Iterazione Documenti
            shard = self._get_dataset_shard(ds, rank, world_size)
            if not shard:
                continue

            with tqdm(total=self.limit if self.limit != -1 else None, 
                      disable=not self.doc_progress, 
                      desc=f"Rank {rank} - {config or 'default'}") as pbar:
                li = 0
                for batch in shard.iter(self.batch_size):
                    if self.limit != -1 and li >= self.limit:
                        break
                    
                    # Trasformazione batch in riga singola
                    for line in (dict(zip(batch, t)) for t in zip(*batch.values())):
                        if self.limit != -1 and li >= self.limit:
                            break
                        
                        # Passiamo full_ds_name come source_file
                        document = self.get_document_from_dict(line, full_ds_name, f"{rank:05d}/{li}")
                        
                        if not document:
                            continue
                        
                        # Aggiungiamo metadati extra utili al writer
                        if config:
                            document.metadata["config_name"] = config
                            
                        self.update_doc_stats(document)
                        self.stat_update("documents")
                        
                        yield document
                        
                        li += 1
                        pbar.update()