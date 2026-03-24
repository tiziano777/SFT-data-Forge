# repository/vocab_chat_type_repository.py
import logging
from typing import List, Optional, Dict, Any
from data_class.entity.vocabulary.vocab_chat_type import VocabChatType
from db.impl.postgres.postgres_db_manager import PostgresDBManager

logger = logging.getLogger(__name__)

import os


POSTGRES_DB_SCHEMA = os.getenv("DB_SCHEMA")

class VocabChatTypeRepository:
    def __init__(self, db_manager: PostgresDBManager):
        self.db = db_manager
        self.table = POSTGRES_DB_SCHEMA + "." + "vocab_chat_type"

    def _prepare_params(self, entity: VocabChatType, include_id: bool = False) -> Dict[str, Any]:
        """Prepara i parametri per le tabelle di vocabolario."""
        params = {
            "code": entity.code,
            "description": entity.description,
            "schema_id": entity.schema_id
        }
        if include_id and entity.id:
            params["id"] = entity.id
        return params

    def insert(self, entity: VocabChatType) -> Optional[VocabChatType]:
        """Inserimento sicuro con mappatura automatica del dizionario di ritorno."""
        params = self._prepare_params(entity, include_id=True if entity.id else False)
        cols = ", ".join(params.keys())
        placeholders = ", ".join([f"%({c})s" for c in params.keys()])
        
        query = f"INSERT INTO {self.table} ({cols}) VALUES ({placeholders}) RETURNING *"
        
        with self.db as db:
            row = db.execute_and_return(query, params)
            return VocabChatType(**row) if row else None

    def get_by_id(self, id: str) -> Optional[VocabChatType]:
        query = f"SELECT * FROM {self.table} WHERE id = %s"
        with self.db as db:
            row = db.execute_and_return(query, (id,))
            return VocabChatType(**row) if row else None

    def get_by_code(self, code: str) -> Optional[VocabChatType]:
        query = f"SELECT * FROM {self.table} WHERE code = %s"
        with self.db as db:
            row = db.execute_and_return(query, (code,))
            return VocabChatType(**row) if row else None

    def get_all(self) -> List[VocabChatType]:
        query = f"SELECT * FROM {self.table} ORDER BY code ASC"
        with self.db as db:
            rows = db.query(query)
            return [VocabChatType(**row) for row in rows]

    def update(self, entity: VocabChatType) -> int:
        """Aggiornamento tramite Named Placeholders per evitare errori di posizionamento."""
        if not entity.id:
            raise ValueError("ID mancante per l'aggiornamento del vocabolario")
            
        params = self._prepare_params(entity, include_id=True)
        set_clauses = ", ".join([f"{col} = %({col})s" for col in params.keys() if col != 'id'])
        
        query = f"UPDATE {self.table} SET {set_clauses} WHERE id = %(id)s"
        
        with self.db as db:
            return db.execute_command(query, params)

    def delete(self, id: str) -> int:
        query = f"DELETE FROM {self.table} WHERE id = %s"
        with self.db as db:
            return db.execute_command(query, (id,))