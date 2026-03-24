from db.impl.postgres.loader.postgres_db_loader import get_db_manager
from pathlib import Path
import os, dotenv

# Resolve project root from this file's location (db/impl/postgres/loader/)
_PROJECT_ROOT = Path(__file__).resolve().parents[4]

dotenv_path = _PROJECT_ROOT / "docker" / "dev" / ".env.dev"
dotenv.load_dotenv(dotenv_path=str(dotenv_path))

# Load paths and schema from the .env file
raw_path = os.getenv("BINDED_RAW_DATA_DIR")
processed_path = os.getenv("BINDED_PROCESSED_DATA_DIR")
mapped_path = os.getenv("BINDED_MAPPED_DATA_DIR")
schema = os.getenv("DB_SCHEMA")

_DDL_DIR = _PROJECT_ROOT / "config" / "postgres_DDL"
vocabularies_file_path = str(_DDL_DIR / "vocabularies.sql")
tables_file_path = str(_DDL_DIR / "tables.sql")
history_file_path = str(_DDL_DIR / "history_tables.sql")
triggers_file_path = str(_DDL_DIR / "triggers.sql")
lineage_fn_file_path = str(_DDL_DIR / "lineage.sql")
samples_file_path = str(_PROJECT_ROOT / "config" / "postgres-insert-DDL.sql")

db = get_db_manager()

with db:
    # --- 1. PREVENTIVE CLEANUP ---
    try:
        # Use execute_command to ensure transactional management
        db.execute_command(f"DROP SCHEMA IF EXISTS {schema} CASCADE;")
        print(f"Schema {schema} dropped (preventive cleanup).")
    except Exception as e:
        print(f"No schema to drop, proceeding: {e}")

    # --- 2. SCHEMA CREATION ---
    db.execute_command(f"CREATE SCHEMA IF NOT EXISTS {schema};")
    print(f"Schema {schema} created successfully.")

    # --- 3. DYNAMIC ENVIRONMENT SETUP ---
    # Now that the schema exists, set the search_path
    db._execute(f"SET search_path TO {schema}, public;")
    print(f"Environment set to schema: {schema}")

    # --- 4. DDL LOADING ---
    db.create_schema_from_file(vocabularies_file_path)
    print(f"Vocabularies for {schema} created successfully from DDL file.")

    db.create_schema_from_file(tables_file_path)
    print(f"Tables for {schema} created successfully from DDL file.")

    db.create_schema_from_file(history_file_path)
    print(f"History tables for {schema} created successfully from DDL file.")

    db.create_schema_from_file(triggers_file_path)
    print(f"Triggers for {schema} created successfully from DDL file.")

    db.create_schema_from_file(lineage_fn_file_path)
    print(f"Lineage for {schema} created successfully from DDL file.")

    # --- 5. INITIAL DATA POPULATION ---
    config_data = [
        ('RAW', raw_path, 1),
        ('PROCESSED', processed_path, 2),
        ('MAPPED', mapped_path, 3)
    ]

    for layer, path, step in config_data:
        if path:
            query = "INSERT INTO config_paths (layer_name, path_prefix, step_value) VALUES (%s, %s, %s)"
            db._execute(query, (layer, path, step))

    print(f"Path configuration loaded dynamically from .env")

    db.create_schema_from_file(samples_file_path)
    print(f"Initial data inserted successfully from insert DDL file.")