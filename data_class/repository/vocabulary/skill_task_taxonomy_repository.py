from typing import List
import logging
from db.impl.postgres.postgres_db_manager import PostgresDBManager
logger = logging.getLogger(__name__)

import os
POSTGRES_DB_SCHEMA = os.getenv("DB_SCHEMA", "MLdatasets")

class SkillTaskTaxonomyRepository:
    def __init__(self, db_manager: PostgresDBManager):
        self.db = db_manager
        self.table = POSTGRES_DB_SCHEMA + "." + "skill_task_taxonomy"

    def get_skills_by_tasks(self, task_codes: List[str]) -> List[str]:
        """Restituisce la lista di skill_code associate alle task fornite."""
        if not task_codes:
            return []
        placeholders = ','.join(['%s'] * len(task_codes))
        query = f"SELECT DISTINCT skill_code FROM {self.table} WHERE task_code IN ({placeholders})"
        with self.db as db:
            rows = db.query(query, tuple(task_codes))
            return [r['skill_code'] for r in rows] if rows else []
