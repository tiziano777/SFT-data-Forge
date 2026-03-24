# datatrove_pipeline/processed_pipeline/reader/extensions/warc_reader.py
from datatrove.pipeline.readers.warc import WarcReader, process_record
from datatrove.data import DocumentsPipeline
from typing import Iterator

from tqdm import tqdm
from datatrove.utils.logging import logger


class CustomWarcReader(WarcReader):
    """
    Reader personalizzato per file WARC che utilizza l'adapter di default
    per estrarre metadati core e generare ID univoci.
    """
    async def _html_to_md(self, html: str) -> str:
        from crawl4ai import AsyncWebCrawler, CrawlerRunConfig
        from crawl4ai.markdown_generation_strategy import DefaultMarkdownGenerator
        #print("Converting HTML to Markdown...")
        #print(html)
        url = f"raw://{html}"
        config = CrawlerRunConfig(
            markdown_generator=DefaultMarkdownGenerator()
        )
        async with AsyncWebCrawler() as crawler:
            result = await crawler.arun(url, config=config)
            if result.success:
                #print("Conversion successful:")
                #print(result.markdown)
                #exit(0)
                return str(result.markdown)
            else:
                raise RuntimeError(f"Failed: {result.error_message}")

    def _default_adapter(self, data: dict, path: str, id_in_file: int | str) -> dict:
        import sys
        sys.path.append('..')
        from ..stats_functions.core_metadata import _clean_data_dict, _extract_text_from_data, _compute_id_hash, _detect_lang
        import asyncio

        document= dict()
        #print("Using default adapter...")
        #print(self.default_metadata)
        #print(data, path, id_in_file)
        cleaned_data = _clean_data_dict(data)
        text_content = _extract_text_from_data(cleaned_data)
        document_id = _compute_id_hash(text_content)

        document['id']= document_id
        document['text']= text_content

        document['metadata']= dict()
        document['metadata']['data'] = cleaned_data
        
        default_metadata = self.default_metadata or {}

        file_name = path.split('/')[-1]
        path_parts = path.split('/')
        subpath = '/'.join(path_parts[:-1]) if len(path_parts) > 0 else ''
        
        document['metadata']['_dataset_name'] = default_metadata.get("_dataset_name", "unknown")
        document['metadata']['_filename'] = file_name
        document['metadata']['_dataset_path'] = default_metadata.get("_dataset_path", "")
        document['metadata']['_subpath'] = subpath
        document['metadata']['_id_hash'] = document_id

        #print("Document before HTML to MD conversion:")
        #print(document)

        html = document['metadata']['data']['text']

        # Se c’è già un event loop (es. Streamlit, Jupyter)
        try:
            loop = asyncio.get_running_loop()
            md = loop.run_until_complete(self._html_to_md(html))
        except RuntimeError:
            # Nessun loop → usiamo asyncio.run()
            md = asyncio.run(self._html_to_md(html))

        available_langs = default_metadata.get("_available_languages", [])
        iso_lang = _detect_lang(
            md[:250] if len(md) > 250 else md, 
            *available_langs
        )
        
        document['metadata']['_lang'] = iso_lang
        document['metadata']['data']['text'] = md

        return document

    def read_file(self, filepath: str) -> Iterator[dict]:
        """
        Legge un file WARC e yield ogni record come documento.
        
        Args:
            filepath: Percorso del file WARC
            
        Yields:
            Document: Documenti processati
        """
        from warcio.archiveiterator import ArchiveIterator

        with self.data_folder.open(filepath, "rb", compression=self.compression) as f:
            for ri, record in enumerate(ArchiveIterator(f)):
                with self.track_time():
                    # Processa il record WARC per estrarre dati base
                    extracted_data = process_record(record)
                    if not extracted_data or len(extracted_data.get('text')) < 10:
                        continue
                    
                    # Crea il documento usando l'adapter personalizzato
                    document = self.get_document_from_dict(extracted_data, filepath, ri)
                    if not document:
                        continue
                    
                    yield document

    def get_document_from_dict(self, data: dict, source_file: str, id_in_file: int):
        document = super().get_document_from_dict(data, source_file, id_in_file)

        if document:
            document.metadata.setdefault("file_path", self.data_folder.resolve_paths(source_file))
        
        return document

    def run(self, data: DocumentsPipeline = None, rank: int = 0, world_size: int = 1) -> DocumentsPipeline:
        """
        Esegue il reader WARC personalizzato.
        
        Args:
            data: Pipeline di documenti esistente (None per il primo step)
            rank: Rank del worker corrente
            world_size: Numero totale di workers
            
        Yields:
            Document: Documenti processati con metadati core
        """
        # Yield eventuali documenti esistenti dalla pipeline
        if data:
            yield from data
            
        # Ottieni lo shard di file per questo worker
        files_shard = (
            self.data_folder.get_shard(rank, world_size, recursive=self.recursive, glob_pattern=self.glob_pattern)
            if not self.paths_file
            else list(self.get_shard_from_paths_file(self.paths_file, rank, world_size))
        )
        
        if files_shard is None:
            raise RuntimeError(f"No files found on {self.data_folder.path}!")
        elif len(files_shard) == 0:
            logger.warning(f"No files found on {self.data_folder.path} for {rank=}")

        # Mescola i file se richiesto
        if self.shuffle_files:
            import random
            random.shuffle(files_shard)
            
        # Processa ogni file nello shard
        li = 0
        skipped = 0
        with (
            tqdm(
                total=self.limit if self.limit != -1 else None,
                desc="Document progress",
                unit="doc",
                disable=not self.doc_progress,
            ) as doc_pbar,
            tqdm(total=len(files_shard), desc="File progress", unit="file", disable=not self.file_progress) as file_pbar,
        ):
            for i, filepath in enumerate(files_shard):
                self.stat_update("input_files")
                logger.info(f"Reading input file {filepath}, {i + 1}/{len(files_shard)}")
                di = 0
                ndocs = 0
                
                # Leggi e processa ogni record nel file WARC
                for di, document in enumerate(self.read_file(filepath)):
                    if skipped < self.skip:
                        skipped += 1
                        continue
                    if self.limit != -1 and li >= self.limit:
                        break
                    
                    # Aggiorna statistiche e yield il documento
                    self.update_doc_stats(document)
                    yield document
                    
                    doc_pbar.update()
                    li += 1
                    ndocs += 1
                
                file_pbar.update()
                self.stat_update("documents", value=ndocs, unit="input_file")
                
                if self.limit != -1 and li >= self.limit:
                    break     


'''
reader = CustomWarcReader(
    data_folder="<PROJECT_ROOT>/nfs/data-download/velvet_v1/COVID_warc_example_dataset/dist_1/",
    default_metadata={
        "_dataset_name": "covid_warc_dataset",
        "_dataset_path": "<PROJECT_ROOT>/nfs/data-download/velvet_v1/COVID_warc_example_dataset",
        "_available_languages": ["en", "it"],
        "_filename": "COVID.warc",
        "_subpath": "dist_1", 
    }
)

# Il reader può essere usato in una pipeline DataTrove
for document in reader():
    print(document)
    exit(0)
'''
