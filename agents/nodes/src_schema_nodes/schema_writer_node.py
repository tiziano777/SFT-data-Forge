import os


POSTGRES_DB = os.getenv("POSTGRES_DB")
POSTGRES_USER = os.getenv("POSTGRES_USER")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD")
POSTGRES_HOST = os.getenv("POSTGRES_HOST")
POSTGRES_PORT = os.getenv("POSTGRES_PORT")
BASE_PREFIX = os.getenv("BASE_PREFIX")
POSTGRES_DB_SCHEMA = os.getenv("DB_SCHEMA")

import json
from agents.states.src_schema_state import State
from db.impl.postgres.postgres_db_manager import PostgresDBManager 


class SchemaWriter:
    def __init__(self, log_path: str = "logs/schema_generation_log.json"):
        self.log_path = log_path
        os.makedirs(os.path.dirname(self.log_path), exist_ok=True)
        self.db_manager = PostgresDBManager(
            dbname=POSTGRES_DB,
            user=POSTGRES_USER,
            password=POSTGRES_PASSWORD,
            host=POSTGRES_HOST,
            port=POSTGRES_PORT
        )

    def __call__(self, state: State) -> State:
        if state.generated_schema and state.valid:
            # path del metadata.json
            output_path = state.output_path

            # Aggiorna la sezione src_schema
            src_schema=state.generated_schema
            
            # Prepara valori da aggiornare nel DB
            set_clauses = ["src_schema = %s"]
            where_clause = "uri = %s"
            params = (json.dumps(src_schema), BASE_PREFIX + os.path.abspath(output_path))

            with self.db_manager as db:
                db.update(
                    table=POSTGRES_DB_SCHEMA + "."+"distribution",
                    set_clauses=set_clauses,
                    where=where_clause,
                    params=params
                )

            # Logga la generazione
            with open(self.log_path, "a", encoding="utf-8") as f:
                f.write(json.dumps(state.model_dump(), ensure_ascii=False) + '\n')
        else:
            print("No valid schema to write.")
            state.valid = False
            state.error_messages.append("No valid schema to write.")
        return state