# repository/dataset_card_repository.py
import logging
from typing import List, Optional, Dict, Any
from db.impl.postgres.postgres_db_manager import PostgresDBManager
from data_class.entity.table.dataset_card import DatasetCard

logger = logging.getLogger(__name__)

import os


POSTGRES_DB_SCHEMA = os.getenv("DB_SCHEMA") or "MLdatasets"

class DatasetCardRepository:
    def __init__(self, db_manager: PostgresDBManager):
        self.db = db_manager
        self.table = POSTGRES_DB_SCHEMA + "." + "dataset_card"

    def _prepare_params(self, entity: DatasetCard, include_id: bool = False) -> Dict[str, Any]:
        """
        Mapping tra Entità e Database. 
        Psycopg 3 gestisce nativamente liste (ARRAY) e dizionari (JSONB).
        """
        params = {
            "dataset_name": entity.dataset_name,
            "dataset_id": entity.dataset_id,
            "modality": entity.modality,
            "dataset_description": entity.dataset_description,
            "publisher": entity.publisher,
            "notes": entity.notes,
            "source_url": entity.source_url,
            "download_url": entity.download_url,
            "languages": entity.languages if isinstance(entity.languages, list) else [],
            "license": entity.license,
            "core_skills": entity.core_skills if isinstance(entity.core_skills, list) else [],
            "tasks": entity.tasks if isinstance(entity.tasks, list) else [],
            "has_reasoning": bool(entity.has_reasoning),
            "quality": entity.quality if entity.quality is not None else 1
        }
        if include_id:
            params["id"] = entity.id
        return params

    def insert(self, entity: DatasetCard) -> Optional[DatasetCard]:
        """Inserimento pulito e mappatura automatica del risultato."""
        params = self._prepare_params(entity)
        cols = ", ".join(params.keys())
        placeholders = ", ".join([f"%({c})s" for c in params.keys()])
        
        query = f"INSERT INTO {self.table} ({cols}) VALUES ({placeholders}) RETURNING *"
        
        try:
            with self.db as db:
                row = db.execute_and_return(query, params)
                return DatasetCard(**row) if row else None
        except Exception as e:
            logger.error(f"Errore durante l'inserimento del DatasetCard: {e}")
            raise

    def get_by_id(self, id: str) -> Optional[DatasetCard]:
        query = f"SELECT * FROM {self.table} WHERE id = %s"
        with self.db as db:
            row = db.execute_and_return(query, (id,))
            return DatasetCard(**row) if row else None

    def exists_by_dataset_id(self, dataset_id: str) -> bool:
        query = f"SELECT 1 FROM {self.table} WHERE dataset_id = %s"
        with self.db as db:
            return db.execute_and_return(query, (dataset_id,)) is not None

    def exists_by_name(self, dataset_name: str) -> bool:
        query = f"SELECT 1 FROM {self.table} WHERE dataset_name = %s"
        with self.db as db:
            return db.execute_and_return(query, (dataset_name,)) is not None

    def get_by_dataset_id(self, dataset_id: str) -> Optional[DatasetCard]:
        query = f"SELECT * FROM {self.table} WHERE dataset_id = %s"
        with self.db as db:
            row = db.execute_and_return(query, (dataset_id,))
            return DatasetCard(**row) if row else None

    def get_by_name(self, dataset_name: str) -> Optional[DatasetCard]:
        query = f"SELECT * FROM {self.table} WHERE dataset_name = %s"
        with self.db as db:
            row = db.execute_and_return(query, (dataset_name,))
            return DatasetCard(**row) if row else None

    def get_all(self) -> List[DatasetCard]:
        query = f"SELECT * FROM {self.table} ORDER BY created_at DESC"
        with self.db as db:
            rows = db.query(query)
            return [DatasetCard(**row) for row in rows]

    def update(self, entity: DatasetCard) -> int:
        """Update con named placeholders e gestione automatica dei tempi."""
        if not entity.id:
            raise ValueError("ID mancante per l'aggiornamento")
            
        params = self._prepare_params(entity, include_id=True)
        
        # Generiamo i set clauses escludendo l'id
        set_clauses = [f"{c} = %({c})s" for c in params.keys() if c != "id"]
        # Aggiungiamo l'aggiornamento manuale del timestamp
        set_clauses.append("last_update = CURRENT_DATE")
        
        query = f"UPDATE {self.table} SET {', '.join(set_clauses)} WHERE id = %(id)s"
        
        with self.db as db:
            return db.execute_command(query, params)

    def delete(self, id: str) -> int:
        query = f"DELETE FROM {self.table} WHERE id = %s"
        with self.db as db:
            return db.execute_command(query, (id,))