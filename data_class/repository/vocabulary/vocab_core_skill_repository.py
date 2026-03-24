# data_class/repository/vocabulary/vocab_core_skill_repository.py
import logging
from typing import List, Optional, Dict, Any
from data_class.entity.vocabulary.vocab_core_skill import VocabCoreSkill
from db.impl.postgres.postgres_db_manager import PostgresDBManager

logger = logging.getLogger(__name__)

import os


POSTGRES_DB_SCHEMA = os.getenv("DB_SCHEMA", "MLdatasets")

class VocabCoreSkillRepository:
    def __init__(self, db_manager: PostgresDBManager):
        self.db = db_manager
        self.table = POSTGRES_DB_SCHEMA + "." + "vocab_core_skill"

    def _prepare_params(self, entity: VocabCoreSkill, include_id: bool = False) -> Dict[str, Any]:
        """Prepara i parametri mappando l'entità VocabCoreSkill."""
        params = {
            "code": entity.code,
            "description": entity.description
        }
        if include_id and entity.id:
            params["id"] = entity.id
        return params

    def insert(self, entity: VocabCoreSkill) -> Optional[VocabCoreSkill]:
        """Inserimento order-agnostic con mapping automatico del risultato."""
        params = self._prepare_params(entity, include_id=True if entity.id else False)
        cols = ", ".join(params.keys())
        placeholders = ", ".join([f"%({c})s" for c in params.keys()])
        
        query = f"INSERT INTO {self.table} ({cols}) VALUES ({placeholders}) RETURNING *"
        
        with self.db as db:
            row = db.execute_and_return(query, params)
            return VocabCoreSkill(**row) if row else None

    def get_by_id(self, id: str) -> Optional[VocabCoreSkill]:
        query = f"SELECT * FROM {self.table} WHERE id = %s"
        with self.db as db:
            row = db.execute_and_return(query, (id,))
            return VocabCoreSkill(**row) if row else None

    def get_by_code(self, code: str) -> Optional[VocabCoreSkill]:
        query = f"SELECT * FROM {self.table} WHERE code = %s"
        with self.db as db:
            row = db.execute_and_return(query, (code,))
            return VocabCoreSkill(**row) if row else None

    def get_all(self) -> List[VocabCoreSkill]:
        query = f"SELECT * FROM {self.table} ORDER BY code ASC"
        with self.db as db:
            rows = db.query(query)
            return [VocabCoreSkill(**row) for row in rows]

    def update(self, entity: VocabCoreSkill) -> int:
        """Aggiorna la skill core usando Named Placeholders."""
        if not entity.id:
            raise ValueError("ID mancante per l'aggiornamento della core skill")
            
        params = self._prepare_params(entity, include_id=True)
        set_clauses = ", ".join([f"{col} = %({col})s" for col in params.keys() if col != 'id'])
        
        query = f"UPDATE {self.table} SET {set_clauses} WHERE id = %(id)s"
        
        with self.db as db:
            return db.execute_command(query, params)

    def delete(self, id: str) -> int:
        query = f"DELETE FROM {self.table} WHERE id = %s"
        with self.db as db:
            return db.execute_command(query, (id,))