from typing import List, Optional, Dict, Any
from db.impl.postgres.postgres_db_manager import PostgresDBManager
from data_class.entity.table.strategy import Strategy
import os
import logging

logger = logging.getLogger(__name__)
POSTGRES_DB_SCHEMA = os.getenv("DB_SCHEMA")

class StrategyRepository:
    def __init__(self, db_manager: PostgresDBManager):
        self.db = db_manager
        self.table = f"{POSTGRES_DB_SCHEMA}.strategy"

    def _prepare_params(self, entity: Strategy, include_id: bool = False) -> Dict[str, Any]:
        params = {
            "recipe_id": entity.recipe_id,
            "distribution_id": entity.distribution_id,
            "replication_factor": entity.replication_factor,
            "template_strategy": entity.template_strategy
        }
        if include_id: params["id"] = entity.id
        return params

    def insert(self, entity: Strategy) -> Optional[Strategy]:
        params = self._prepare_params(entity)
        cols = ", ".join(params.keys())
        placeholders = ", ".join([f"%({c})s" for c in params.keys()])
        query = f"INSERT INTO {self.table} ({cols}) VALUES ({placeholders}) RETURNING *"
        with self.db as db:
            row = db.execute_and_return(query, params)
            return Strategy(**row) if row else None
    
    def get_by_id(self, strategy_id: str) -> Optional[Strategy]:
        query = f"SELECT * FROM {self.table} WHERE id = %s"
        with self.db as db:
            params=(strategy_id,)
            row = db.execute_and_return(query, params)
            return Strategy(**row) if row else None

    def update(self, entity: Strategy) -> Optional[Strategy]:
        if not entity.id:
            logger.error("Cannot update Strategy without id")
            return None
        params = self._prepare_params(entity, include_id=True)
        set_clause = ", ".join([f"{k} = %({k})s" for k in params.keys() if k != "id"])
        query = f"UPDATE {self.table} SET {set_clause} WHERE id = %(id)s RETURNING *"
        with self.db as db:
            row = db.execute_and_return(query, params)
            return Strategy(**row) if row else None

    def get_by_recipe_id(self, recipe_id: str) -> List[Strategy]:
        query = f"SELECT * FROM {self.table} WHERE recipe_id = %s"
        with self.db as db:
            rows = db.query(query, (recipe_id,))
            return [Strategy(**row) for row in rows] if rows else []
    
    def delete_by_strategy_id(self, strategy_id: str) -> int:
        query = f"DELETE FROM {self.table} WHERE id = %s"
        with self.db as db:
            params=(strategy_id,)
            return db.execute_command(query, params)
    
    def delete_by_id(self, id: str) -> int:
        query = f"DELETE FROM {self.table} WHERE id = %s"
        with self.db as db:
            params=(id,)
            return db.execute_command(query, params)