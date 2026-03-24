from data_class.entity.table.checkpoint import Checkpoint
import logging
import os
from typing import Optional, Dict, Any
from db.impl.postgres.postgres_db_manager import PostgresDBManager

logger = logging.getLogger(__name__)
POSTGRES_DB_SCHEMA = os.getenv("DB_SCHEMA")

class CheckpointRepository:
    def __init__(self, db_manager: PostgresDBManager):
        self.db = db_manager
        self.table = f"{POSTGRES_DB_SCHEMA}.checkpoint"

    def _prepare_params(self, entity: Checkpoint, include_id: bool = False) -> Dict[str, Any]:
        params = {
            "recipe_id": entity.recipe_id,
            "checkpoint_number": entity.checkpoint_number,
            "src_uri": entity.src_uri,
            "name": entity.name,
            "description": entity.description,
            "results": entity.results, # Psycopg3 converte dict in JSONB
            "hyperparams": entity.hyperparams
        }
        if include_id: params["id"] = entity.id
        return params

    def insert(self, entity: Checkpoint) -> Optional[Checkpoint]:
        params = self._prepare_params(entity)
        cols = ", ".join(params.keys())
        placeholders = ", ".join([f"%({c})s" for c in params.keys()])
        query = f"INSERT INTO {self.table} ({cols}) VALUES ({placeholders}) RETURNING *"
        with self.db as db:
            row = db.execute_and_return(query, params)
            return Checkpoint(**row) if row else None