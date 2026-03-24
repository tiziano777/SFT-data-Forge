from typing import List, Optional, Dict, Any
from db.impl.postgres.postgres_db_manager import PostgresDBManager
from data_class.entity.table.strategy_system_prompt import StrategySystemPrompt
import os
import logging

logger = logging.getLogger(__name__)
POSTGRES_DB_SCHEMA = os.getenv("DB_SCHEMA")

class StrategySystemPromptRepository:
    def __init__(self, db_manager: PostgresDBManager):
        self.db = db_manager
        self.table = f"{POSTGRES_DB_SCHEMA}.strategy_system_prompt"

    def _prepare_params(self, entity: StrategySystemPrompt, include_id: bool = False) -> Dict[str, Any]:
        params = {
            "strategy_id": entity.strategy_id,
            "system_prompt_name": entity.system_prompt_name,
        }
        if include_id: params["id"] = entity.id
        return params

    def insert(self, entity: StrategySystemPrompt) -> Optional[StrategySystemPrompt]:
        params = self._prepare_params(entity)
        cols = ", ".join(params.keys())
        placeholders = ", ".join([f"%({c})s" for c in params.keys()])
        query = f"INSERT INTO {self.table} ({cols}) VALUES ({placeholders}) RETURNING *"
        with self.db as db:
            row = db.execute_and_return(query, params)
            return StrategySystemPrompt(**row) if row else None

    def get_by_strategy_id(self, strategy_id: str) -> List[StrategySystemPrompt]:
        query = f"SELECT * FROM {self.table} WHERE strategy_id = %s"
        with self.db as db:
            rows = db.query(query, (strategy_id,))
            return [StrategySystemPrompt(**row) for row in rows]
    
    def delete_by_strategy_id(self, strategy_id: str) -> int:
        query = f"DELETE FROM {self.table} WHERE strategy_id = %s"
        with self.db as db:
            return db.execute_command(query, (strategy_id,))