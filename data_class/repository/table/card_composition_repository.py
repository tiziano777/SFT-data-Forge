# repository/card_composition_repository.py
import logging
from typing import List, Optional, Dict, Any
from db.impl.postgres.postgres_db_manager import PostgresDBManager
from data_class.entity.table.card_composition import CardComposition

logger = logging.getLogger(__name__)

import os


POSTGRES_DB_SCHEMA = os.getenv("DB_SCHEMA")

class CardCompositionRepository:
    def __init__(self, db_manager: PostgresDBManager):
        self.db = db_manager
        self.table = POSTGRES_DB_SCHEMA + "." + "card_composition"

    def _prepare_params(self, entity: CardComposition) -> Dict[str, Any]:
        """Prepara il dizionario per i Named Placeholders."""
        return {
            'parent_card_name': entity.parent_card_name,
            'child_card_name': entity.child_card_name,
            'weight': entity.weight
        }

    def insert(self, entity: CardComposition) -> Optional[CardComposition]:
        """Inserimento order-agnostic con Named Placeholders."""
        params = self._prepare_params(entity)
        cols = ", ".join(params.keys())
        placeholders = ", ".join([f"%({c})s" for c in params.keys()])
        
        query = f"INSERT INTO {self.table} ({cols}) VALUES ({placeholders}) RETURNING *"
        
        try:
            with self.db as db:
                row = db.execute_and_return(query, params)
                return CardComposition(**row) if row else None
        except Exception as e:
            logger.error(f"Errore durante l'inserimento della composizione: {e}")
            raise

    def get_by_pk(self, parent_name: str, child_name: str) -> Optional[CardComposition]:
        """Recupera una specifica relazione tramite Primary Key composta."""
        query = f"SELECT * FROM {self.table} WHERE parent_card_name = %s AND child_card_name = %s"
        with self.db as db:
            row = db.execute_and_return(query, (parent_name, child_name))
            return CardComposition(**row) if row else None

    def get_children_by_parent(self, parent_name: str) -> List[CardComposition]:
        """Trova tutti i figli di un MIX (parent)."""
        query = f"SELECT * FROM {self.table} WHERE parent_card_name = %s"
        with self.db as db:
            rows = db.query(query, (parent_name,))
            return [CardComposition(**row) for row in rows]

    def get_parents_by_child(self, child_name: str) -> List[CardComposition]:
        """Trova tutti i MIX che contengono questo figlio."""
        query = f"SELECT * FROM {self.table} WHERE child_card_name = %s"
        with self.db as db:
            rows = db.query(query, (child_name,))
            return [CardComposition(**row) for row in rows]

    def update(self, entity: CardComposition) -> int:
        """Aggiorna il peso della relazione usando PK composta."""
        params = self._prepare_params(entity)
        query = f"""
            UPDATE {self.table} 
            SET weight = %(weight)s 
            WHERE parent_card_name = %(parent_card_name)s 
              AND child_card_name = %(child_card_name)s
        """
        with self.db as db:
            return db.execute_command(query, params)

    def delete(self, parent_name: str, child_name: str) -> int:
        """Rimuove la relazione tra due card."""
        query = f"DELETE FROM {self.table} WHERE parent_card_name = %s AND child_card_name = %s"
        with self.db as db:
            return db.execute_command(query, (parent_name, child_name))