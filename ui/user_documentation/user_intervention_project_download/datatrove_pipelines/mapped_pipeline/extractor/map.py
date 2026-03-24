import sys
sys.path.append("../../..")
from datatrove.data import DocumentsPipeline
from datatrove.pipeline.extractors.base import BaseExtractor
from datatrove.utils.logging import logger

from mappings.mapper import Mapper
from typing import Dict, List, Any

MappingSpec = Dict[str, List[Any]]

class MapperExtractor(BaseExtractor):
    """Extended Extractor that processes documents using a mapper on metadata.data field"""

    type = "🛢 - MAPPER"

    def __init__(self, timeout: float = 1, mapping_spec: MappingSpec = None, dst_schema: str = Dict, src_schema: Dict = None):
        """
        Args:
            mapper_class: La classe mapper da utilizzare per processare metadata.data
            timeout: Timeout per l'estrazione
            mapping_spec: Specifica di mapping per il Mapper
            dst_schema: Schema di destinazione per il Mapper
            src_schema: Schema di origine per il Mapper
        """
        super().__init__(timeout=timeout)
        self.mapper = Mapper(mapping_spec=mapping_spec, dst_schema=dst_schema, src_schema=src_schema)

    def extract(self, text: str) -> str:
        """abstract method that actually implements the extraction
        Args:
          text: str: non-plain text
        Returns: extracted plain text
        """
        pass

    def run(self, data: DocumentsPipeline, rank: int = 0, world_size: int = 1) -> DocumentsPipeline:
        """Iterates through each document and processes metadata.data field with mapper

        Args:
            data: DocumentsPipeline: Stream di documenti con campi id, text, metadata
            rank: int: Rank del processo (Default: 0)
            world_size: int: World size (Default: 1)

        Returns:
            DocumentsPipeline: Stream di documenti processati
        """
        for doc in data:
            with self.track_time():
                try:
                    if hasattr(doc, 'metadata') and doc.metadata:
                        # Processa metadata.data con il mapper
                        #print(doc.metadata)
                        mapped_data, succes, error = self.mapper.apply_mapping(doc.metadata)
                        if not succes:
                            logger.warning(f"❌ Mapping failed for document ID {doc.id} with error: {error}")
                            print(error)
                            print(doc.metadata)
                            exit(1)
                        #else:
                            #print(f"🗺️ Mapped document ID: {doc.id}")
                            #print(mapped_data)

                        doc.metadata['data'] = mapped_data
                    
                except TimeoutError:
                    logger.warning("⏰ Timeout while processing metadata. Skipping record.")
                    continue
                except Exception as e:
                    if not self._warned_error:
                        logger.warning(
                            f'❌ Error "{e}" while processing metadata. Skipping record. '
                            f"This message will only appear once."
                        )
                        self._warned_error = True
                    continue

            yield doc
    
    def __call__(self, data: DocumentsPipeline, rank: int = 0, world_size: int = 1) -> DocumentsPipeline:
        """Chiamabile come funzione"""
        return self.run(data=data, rank=rank, world_size=world_size)


