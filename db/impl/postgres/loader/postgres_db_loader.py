import os
from db.impl.postgres.postgres_db_manager import PostgresDBManager

def get_db_manager() -> PostgresDBManager:
    """Restituisce un'istanza di PostgresDBManager con parametri da variabili d'ambiente."""
    dbname = os.getenv("POSTGRES_DB")
    user = os.getenv("POSTGRES_USER")
    password = os.getenv("POSTGRES_PASSWORD")
    host = os.getenv("POSTGRES_HOST")
    port = int(os.getenv("POSTGRES_PORT"))

    if not all([dbname, user, password]):
        raise ValueError("⚠️ Variabili d'ambiente DB mancanti: POSTGRES_DB, POSTGRES_USER, POSTGRES_PASSWORD")
    db = PostgresDBManager(dbname, user, password, host, port)
    db.connect()
    return db
