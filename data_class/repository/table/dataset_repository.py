# repository/dataset_repository.py
import logging
from typing import List, Optional, Dict, Any
from data_class.entity.table.dataset import Dataset
from db.impl.postgres.postgres_db_manager import PostgresDBManager

logger = logging.getLogger(__name__)

import os


POSTGRES_DB_SCHEMA = os.getenv("DB_SCHEMA")

class DatasetRepository:
    def __init__(self, db_manager: PostgresDBManager):
        self.db = db_manager
        self.table = POSTGRES_DB_SCHEMA + "." + "dataset"

    def get_by_uri_prefix_or_step(self, uri_prefix: str, step: int) -> List[Dataset]:
        """
        Estrae dataset tramite match del path (LIKE) OPPURE tramite uno step specifico.
        Include sia dataset virtuali che fisici.
        """
        query = f"""
            SELECT * FROM {self.table} 
            WHERE uri LIKE %(prefix)s OR step = %(step)s
        """
        params = {"prefix": f"{uri_prefix}%", "step": step}
        with self.db as db:
            rows = db.query(query, params)
            return [Dataset(**row) for row in rows]

    def _prepare_params(self, entity: Dataset, include_id: bool = False) -> Dict[str, Any]:
        """
        Prepara i parametri per il DB. 
        Psycopg 3 gestisce automaticamente liste (globs, languages) e tipi complessi.
        """
        params = {
            "uri": entity.uri,
            "derived_card": entity.derived_card if entity.derived_card and entity.derived_card else None,
            "derived_dataset": entity.derived_dataset if entity.derived_dataset and entity.derived_dataset else None,
            "globs": entity.globs if isinstance(entity.globs, list) else [],
            "languages": entity.languages if isinstance(entity.languages, list) else [],
            "name": entity.name,
            "description": entity.description,
            "source": entity.source,
            "version": entity.version,
            "issued": entity.issued,
            "modified": entity.modified,
            "license": entity.license,
            "step": entity.step if entity.step is not None else 1
        }
        if include_id:
            params["id"] = entity.id
        return params

    def insert(self, entity: Dataset) -> Optional[Dataset]:
        """Inserimento order-agnostic con Named Placeholders."""
        params = self._prepare_params(entity)
        cols = ", ".join(params.keys())
        placeholders = ", ".join([f"%({c})s" for c in params.keys()])
        
        query = f"INSERT INTO {self.table} ({cols}) VALUES ({placeholders}) RETURNING *"
        
        with self.db as db:
            row = db.execute_and_return(query, params)
            return Dataset(**row) if row else None

    def upsert_by_uri(self, entity: Dataset) -> Optional[Dataset]:
        """
        Versione ottimizzata usando ON CONFLICT di Postgres.
        Molto più performante e meno soggetta a race conditions.
        """
        params = self._prepare_params(entity)
        cols = ", ".join(params.keys())
        placeholders = ", ".join([f"%({c})s" for c in params.keys()])
        
        # Escludiamo 'uri' dall'aggiornamento perché è la nostra chiave di conflitto
        update_cols = ", ".join([f"{c} = EXCLUDED.{c}" for c in params.keys() if c != 'uri'])

        query = f"""
            INSERT INTO {self.table} ({cols})
            VALUES ({placeholders})
            ON CONFLICT (uri) 
            DO UPDATE SET {update_cols}
            RETURNING *
        """
        with self.db as db:
            row = db.execute_and_return(query, params)
            return Dataset(**row) if row else None
        
    def upsert_by_name(self, entity: Dataset) -> Optional[Dataset]:
        """
        Upsert basato sul campo 'name'. Utile quando la chiave unica 'name' crea conflitti
        ma vogliamo garantire che esista un record correlato al nome specificato.
        """
        params = self._prepare_params(entity)
        cols = ", ".join(params.keys())
        placeholders = ", ".join([f"%({c})s" for c in params.keys()])

        # Escludiamo 'name' dall'aggiornamento perché è la nostra chiave di conflitto
        update_cols = ", ".join([f"{c} = EXCLUDED.{c}" for c in params.keys() if c != 'name'])

        query = f"""
            INSERT INTO {self.table} ({cols})
            VALUES ({placeholders})
            ON CONFLICT (name)
            DO UPDATE SET {update_cols}
            RETURNING *
        """
        with self.db as db:
            row = db.execute_and_return(query, params)
            return Dataset(**row) if row else None

    def update(self, entity: Dataset) -> int:
        """Aggiorna un dataset esistente tramite ID."""
        if not entity.id:
            raise ValueError("L'entità deve avere un ID per essere aggiornata")
        
        params = self._prepare_params(entity, include_id=True)
        set_clauses = ", ".join([f"{col} = %({col})s" for col in params.keys() if col != 'id'])
        
        query = f"UPDATE {self.table} SET {set_clauses} WHERE id = %(id)s"
        
        with self.db as db:
            return db.execute_command(query, params)

    def get_by_id(self, id: str) -> Optional[Dataset]:
        query = f"SELECT * FROM {self.table} WHERE id = %s"
        with self.db as db:
            row = db.execute_and_return(query, (id,))
            return Dataset(**row) if row else None

    def get_by_uri(self, uri: str) -> Optional[Dataset]:
        query = f"SELECT * FROM {self.table} WHERE uri = %s"
        with self.db as db:
            row = db.execute_and_return(query, (uri,))
            return Dataset(**row) if row else None

    def get_by_name(self, name: str) -> Optional[Dataset]:
        query = f"SELECT * FROM {self.table} WHERE name = %s"
        with self.db as db:
            row = db.execute_and_return(query, (name,))
            return Dataset(**row) if row else None
    
    def exists_by_name(self, name: str) -> bool:
        query = f"SELECT 1 FROM {self.table} WHERE name = %s LIMIT 1"
        with self.db as db:
            row = db.execute_and_return(query, (name,))
            return bool(row)

    def get_by_step(self, step: int) -> List[Dataset]:
        query = f"SELECT * FROM {self.table} WHERE step = %s"
        with self.db as db:
            rows = db.query(query, (step,))
            return [Dataset(**row) for row in rows]

    def get_all(self) -> List[Dataset]:
        query = f"SELECT * FROM {self.table}"
        with self.db as db:
            rows = db.query(query)
            return [Dataset(**row) for row in rows]

    def get_by_uri_prefix(self, uri_prefix: str) -> List[Dataset]:
        query = f"SELECT * FROM {self.table} WHERE uri LIKE %s"
        with self.db as db:
            rows = db.query(query, (f"{uri_prefix}%",))
            return [Dataset(**row) for row in rows]

    def get_by_derived_card(self, card_uuid: str) -> Optional[Dataset]:
        """
        Verifica se esiste un dataset fisico (materializzato) 
        che deriva dalla dataset_card specificata.
        """
        query = f"SELECT * FROM {self.table} WHERE derived_card = %s LIMIT 1"
        with self.db as db:
            row = db.execute_and_return(query, (card_uuid,))
            return Dataset(**row) if row else None

    def delete(self, id: str) -> int:
        query = f"DELETE FROM {self.table} WHERE id = %s"
        with self.db as db:
            return db.execute_command(query, (id,))