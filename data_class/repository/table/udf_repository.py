# repository/udf_repository.py
import logging
from typing import List, Optional, Dict, Any
from db.impl.postgres.postgres_db_manager import PostgresDBManager
from data_class.entity.table.udf import Udf

logger = logging.getLogger(__name__)

import os


POSTGRES_DB_SCHEMA = os.getenv("DB_SCHEMA")

class UdfRepository:
    def __init__(self, db_manager: PostgresDBManager):
        self.db = db_manager
        self.table = POSTGRES_DB_SCHEMA + "." + "udf"

    def _prepare_params(self, entity: Udf, include_id: bool = False) -> Dict[str, Any]:
        """
        Prepara i parametri per il DB.
        example_params viene passato come list[str] (ogni str è un json.dumps).
        """
        params = {
            "name": entity.name,
            "description": entity.description,
            "function_definition": entity.function_definition,
            # Passiamo la lista così com'è. Se il DBManager non ha il dumper per 'list', 
            # Psycopg la tratterà come array nativo.
            "example_params": entity.example_params if isinstance(entity.example_params, list) else [],
            "issued": entity.issued,
            "modified": entity.modified
        }
        if include_id and entity.id is not None:
            params["id"] = entity.id
        return params

    def insert(self, entity: Udf) -> Optional[Udf]:
        params = self._prepare_params(entity, include_id=True if entity.id else False)
        cols = ", ".join(params.keys())
        # SOTA: aggiungiamo il cast esplicito ::text[] solo per example_params per blindare la query
        placeholders = ", ".join([
            f"%({c})s::text[]" if c == "example_params" else f"%({c})s" 
            for c in params.keys()
        ])
        
        query = f"INSERT INTO {self.table} ({cols}) VALUES ({placeholders}) RETURNING *"
        
        with self.db as db:
            row = db.execute_and_return(query, params)
            return Udf(**row) if row else None

    def upsert(self, entity: Udf) -> Optional[Udf]:
        params = self._prepare_params(entity, include_id=True if entity.id else False)
        cols = ", ".join(params.keys())
        
        # Costruiamo i placeholders con cast per l'array di testi
        placeholder_list = []
        for c in params.keys():
            if c == "example_params":
                placeholder_list.append(f"%({c})s::text[]")
            else:
                placeholder_list.append(f"%({c})s")
        placeholders = ", ".join(placeholder_list)
        
        conflict_target = "id" if entity.id else "name"
        
        query = f"""
            INSERT INTO {self.table} ({cols})
            VALUES ({placeholders})
            ON CONFLICT ({conflict_target}) 
            DO UPDATE SET 
                name = EXCLUDED.name,
                description = EXCLUDED.description,
                function_definition = EXCLUDED.function_definition,
                example_params = EXCLUDED.example_params::text[],
                modified = EXCLUDED.modified
            RETURNING *
        """
        
        with self.db as db:
            row = db.execute_and_return(query, params)
            return Udf(**row) if row else None

    def update(self, entity: Udf) -> int:
        if not entity.id:
            raise ValueError("ID mancante per l'aggiornamento della UDF")
            
        params = self._prepare_params(entity, include_id=True)
        
        clauses = []
        for col in params.keys():
            if col == 'id': continue
            if col == 'example_params':
                clauses.append(f"{col} = %({col})s::text[]")
            else:
                clauses.append(f"{col} = %({col})s")
        
        set_clauses = ", ".join(clauses)
        query = f"UPDATE {self.table} SET {set_clauses} WHERE id = %(id)s"
        
        with self.db as db:
            return db.execute_command(query, params)

    def get_by_id(self, id: int) -> Optional[Udf]:
        query = f"SELECT * FROM {self.table} WHERE id = %s"
        with self.db as db:
            row = db.execute_and_return(query, (id,))
            return Udf(**row) if row else None

    def get_by_name(self, name: str) -> Optional[Udf]:
        query = f"SELECT * FROM {self.table} WHERE name = %s"
        with self.db as db:
            row = db.execute_and_return(query, (name,))
            return Udf(**row) if row else None

    def get_all(self) -> List[Udf]:
        query = f"SELECT * FROM {self.table} ORDER BY name ASC"
        with self.db as db:
            rows = db.query(query)
            return [Udf(**row) for row in rows]

    def delete(self, id: int) -> int:
        query = f"DELETE FROM {self.table} WHERE id = %s"
        with self.db as db:
            return db.execute_command(query, (id,))