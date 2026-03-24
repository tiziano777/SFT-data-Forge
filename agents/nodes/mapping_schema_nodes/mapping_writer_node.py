import os


POSTGRES_DB = os.getenv("POSTGRES_DB")
POSTGRES_USER = os.getenv("POSTGRES_USER")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD")
POSTGRES_HOST = os.getenv("POSTGRES_HOST")
POSTGRES_PORT = os.getenv("POSTGRES_PORT")
POSTGRES_DB_SCHEMA = os.getenv("DB_SCHEMA")

from agents.states.mapping_schema_state import State
from db.impl.postgres.postgres_db_manager import PostgresDBManager 

class MappingWriter:
    def __init__(self, log_path: str = "logs/mapping_generation_log.json"):
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
        if state.mapping and state.valid:
            try:
                with self.db_manager as db:
                    set_clauses = ["mapping = %s"]
                    where_clause = "id = %s"

                    params = (state.mapping,state.output_path)
                    db.update(
                        table=POSTGRES_DB_SCHEMA + "."+"mapping",
                        set_clauses=set_clauses,
                        where=where_clause,
                        params=params
                    )
            except Exception as e:
                print(f"Errore durante l'update DB: {e}")
                state.error_messages.append(str(e))
        else:
            print("No valid mapping to write.")
            state.valid = False
            state.error_messages.append("No valid mapping to write.")
        return state