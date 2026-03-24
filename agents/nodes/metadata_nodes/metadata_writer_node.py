import os


POSTGRES_DB = os.getenv("POSTGRES_DB")
POSTGRES_USER = os.getenv("POSTGRES_USER")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD")
POSTGRES_HOST = os.getenv("POSTGRES_HOST")
POSTGRES_PORT = os.getenv("POSTGRES_PORT")
BASE_PREFIX = os.getenv("BASE_PREFIX")
POSTGRES_DB_SCHEMA = os.getenv("DB_SCHEMA")

from agents.states.dataset_metadata_state import State
from db.impl.postgres.postgres_db_manager import PostgresDBManager 


class MetadataWriter:
    """
    Nodo finale della pipeline che scrive i metadati generati direttamente nel DB,
    aggiornando i campi chiave di una distribution e aggiungendo metadata_uri.
    """
    def __init__(self, log_path: str = "logs/metadata_generation_log.json"):
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
        print("#############################################")
        print(str(state.metadata))
        print("#############################################")
        print(state.output_path)
        print("##############################################")

        if state.metadata:
            # URI del metadata.json
            distribution_uri = BASE_PREFIX + os.path.abspath(state.output_path) if state.output_path else None

            # Prepara valori da aggiornare nel DB
            update_values = state.metadata.copy()  # copia dei metadati generati


            try:
                with self.db_manager as db:
                    set_clauses = [f"{col} = %s" for col in update_values.keys()]
                    where_clause = "uri = %s"

                    params = tuple(list(update_values.values()) + [distribution_uri])

                    db.update(
                        table=POSTGRES_DB_SCHEMA + "."+"distribution",
                        set_clauses=set_clauses,
                        where=where_clause,
                        params=params
                    )
            except Exception as e:
                print(f"Errore durante l'update DB: {e}")
                state.error_messages.append(str(e))
        else:
            print("No valid metadata to write.")
            state.error_messages.append("No valid metadata to write.")

        return state
