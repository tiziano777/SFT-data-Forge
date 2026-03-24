# repository/distribution_repository.py
import logging
from typing import List, Optional, Any, Dict
from data_class.entity.table.distribution import Distribution
from db.impl.postgres.postgres_db_manager import PostgresDBManager

logger = logging.getLogger(__name__)

import os

class DistributionRepository:
    
    def __init__(self, db_manager: PostgresDBManager):
        self.db = db_manager
        POSTGRES_DB_SCHEMA = os.getenv("DB_SCHEMA")
        self.table = POSTGRES_DB_SCHEMA + "." + "distribution"

    def _sanitize_distribution_values(self, entity: Distribution) -> Dict[str, Any]:
        """Prepara i dati garantendo la compatibilità nativa con Psycopg 3."""
        derived_from = entity.derived_from
        if derived_from in [None, "", "{}", {}]:
            derived_from = None
        
        # Con Psycopg 3, passiamo i dict e list direttamente; il driver gestisce JSONB e ARRAY
        src_schema = entity.src_schema if isinstance(entity.src_schema, dict) else {}
        tags = entity.tags if isinstance(entity.tags, list) else []

        return {
            'uri': entity.uri,
            'tokenized_uri': entity.tokenized_uri,
            'dataset_id': entity.dataset_id,
            'glob': entity.glob,
            'format': entity.format,
            'name': entity.name,
            'query': entity.query,
            'script': entity.script,
            'lang': entity.lang,
            'split': entity.split or "unknown",
            'derived_from': derived_from,
            'src_schema': src_schema, # Passiamo dict nativo
            'description': entity.description,
            'tags': tags,             # Passiamo lista nativa
            'license': entity.license,
            'version': entity.version,
            'issued': entity.issued,
            'modified': entity.modified,
            'materialized': bool(entity.materialized),
            'step': entity.step or 1
        }

    def get_all(self) -> List[Distribution]:
        """
        Restituisce tutte le distributions nella tabella.
        """
        query = f"SELECT * FROM {self.table}"
        with self.db as db:
            rows = db.query(query)
            return [Distribution(**row) for row in rows]

    def update(self, entity: Distribution) -> int:
        """Update sicuro e indipendente dall'ordine delle colonne."""
        if not entity.id:
            raise ValueError("L'entità deve avere un ID per essere aggiornata")
        
        sanitized = self._sanitize_distribution_values(entity)
        params = {**sanitized, 'id': entity.id}
        
        # Costruiamo la clausola SET dinamicamente dalle chiavi
        set_clauses = ", ".join([f"{col} = %({col})s" for col in sanitized.keys()])
        
        query = f"UPDATE {self.table} SET {set_clauses} WHERE id = %(id)s"
        
        with self.db as db:
            return db.execute_command(query, params)

    def insert(self, entity: Distribution) -> Optional[Distribution]:
        """Insert con ritorno dell'entità mappata."""
        sanitized = self._sanitize_distribution_values(entity)
        cols = ", ".join(sanitized.keys())
        placeholders = ", ".join([f"%({col})s" for col in sanitized.keys()])
        
        query = f"INSERT INTO {self.table} ({cols}) VALUES ({placeholders}) RETURNING *"
        
        with self.db as db:
            row = db.execute_and_return(query, sanitized)
            return Distribution(**row) if row else None

    def upsert_by_uri(self, entity: Distribution) -> Optional[Distribution]:
        """
        UPSERT atomico: Inserisce una nuova Distribution o aggiorna quella esistente 
        se l'URI è già presente. Ritorna l'entità aggiornata/inserita.
        """
        sanitized = self._sanitize_distribution_values(entity)
        
        # Costruzione dinamica delle colonne e dei placeholders
        cols = ", ".join(sanitized.keys())
        placeholders = ", ".join([f"%({col})s" for col in sanitized.keys()])
        
        # Generazione della clausola SET per l'aggiornamento
        # Escludiamo 'uri' dalla lista degli aggiornamenti perché è la chiave del conflitto
        update_clauses = ", ".join(
            [f"{col} = EXCLUDED.{col}" for col in sanitized.keys() if col != 'uri']
        )
        
        query = f"""
            INSERT INTO {self.table} ({cols})
            VALUES ({placeholders})
            ON CONFLICT (uri) 
            DO UPDATE SET {update_clauses}
            RETURNING *
        """
        
        with self.db as db:
            row = db.execute_and_return(query, sanitized)
            return Distribution(**row) if row else None

    def get_by_id(self, id: str) -> Optional[Distribution]:
        query = f"SELECT * FROM {self.table} WHERE id = %s"
        with self.db as db:
            row = db.execute_and_return(query, (id,))
            return Distribution(**row) if row else None

    def get_by_uri(self, uri: str) -> Optional[Distribution]:
        query = f"SELECT * FROM {self.table} WHERE uri = %s"
        with self.db as db:
            row = db.execute_and_return(query, (uri,))
            return Distribution(**row) if row else None

    def get_by_dataset_id(self, dataset_id: str) -> List[Distribution]:
        query = f"SELECT * FROM {self.table} WHERE dataset_id = %s"
        with self.db as db:
            rows = db.query(query, (dataset_id,))
            return [Distribution(**row) for row in rows]

    def get_by_uri_prefix_or_step(self, uri_prefix: str, step: int) -> List[Distribution]:
        """
        Estrae distribuzioni (metadati) tramite prefisso URI o step specifico.
        """
        query = f"""
            SELECT * FROM {self.table} 
            WHERE uri LIKE %(prefix)s OR step = %(step)s
        """
        params = {"prefix": f"{uri_prefix}%", "step": step}
        with self.db as db:
            rows = db.query(query, params)
            return [Distribution(**row) for row in rows]

    def get_materialized_by_uri_prefix_or_step(self, uri_prefix: str, step: int) -> List[Distribution]:
        """
        Estrae solo le distribuzioni materializzate (file esistenti su disco/NFS)
        che matchano il path o lo step indicato.
        """
        query = f"""
            SELECT * FROM {self.table} 
            WHERE (uri LIKE %(prefix)s OR step = %(step)s)
            AND materialized = TRUE
        """
        params = {"prefix": f"{uri_prefix}%", "step": step}
        with self.db as db:
            rows = db.query(query, params)
            return [Distribution(**row) for row in rows]

    def get_by_dataset_id_and_materialized(self, dataset_id: str, materialized: bool) -> List[Distribution]:
        query = f"SELECT * FROM {self.table} WHERE dataset_id = %s AND materialized = %s"
        with self.db as db:
            rows = db.query(query, (dataset_id, materialized))
            return [Distribution(**row) for row in rows]