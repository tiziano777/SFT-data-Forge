import json
import logging
import traceback
import dotenv
import os
from pathlib import Path

_PROJECT_ROOT = Path(__file__).resolve().parents[3]
dotenv.load_dotenv(str(_PROJECT_ROOT / "docker" / "dev" / ".env.dev"))  # Load environment variables from .env file
BASE_PREFIX = os.getenv("BASE_PREFIX")

from data_class.entity.table.dataset_card import DatasetCard
from data_class.entity.table.dataset import Dataset
from data_class.entity.table.distribution import Distribution

from data_class.repository.table.dataset_card_repository import DatasetCardRepository
from data_class.repository.table.dataset_repository import DatasetRepository
from data_class.repository.table.distribution_repository import DistributionRepository

from db.impl.postgres.loader.postgres_db_loader import get_db_manager 

# Base logging configuration
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def process_and_insert_jsonl(file_path):
    """
    Reads a JSONL file, maps fields to entities and inserts them into the DB
    respecting referential integrity constraints.
    """
    results = []

    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            dataset_card_repository = DatasetCardRepository(get_db_manager())
            distribution_repository = DistributionRepository(get_db_manager())
            dataset_repository = DatasetRepository(get_db_manager())
                    

            for line_num, line in enumerate(f, 1):
                if not line.strip():
                    continue
                
                row = json.loads(line)
                print(row)
                try:
                    # 1. Create and Insert DatasetCard
                    # Handle data cleanup (e.g. strings -> lists where required by DDL)
                    if not ( dataset_card_repository.exists_by_dataset_id(row.get("card_dataset_id")) or dataset_card_repository.exists_by_name(row.get("card_dataset_name")) ):
                        card = DatasetCard(
                            id=None,
                            dataset_id=row.get("card_dataset_id"),
                            dataset_name=row.get("card_dataset_name"),
                            modality=row.get("card_modality", "text"),
                            dataset_description=row.get("card_dataset_description"),
                            publisher=row.get("card_publisher"),
                            source_url=row.get("card_source_url"),
                            download_url=row.get("card_download_url"),
                            languages=row.get("card_languages", []),
                            license=row.get("card_license", "unknown"),
                            core_skills=row.get("card_core_skills", []),
                            tasks=row.get("card_tasks", []),
                            sources=row.get("card_sources", []),
                            source_type=row.get("card_source_type"),
                            fields=row.get("card_fields", []),
                            vertical=row.get("card_vertical", []),
                            contents=row.get("card_contents", []),
                            has_reasoning=row.get("card_has_reasoning", False),
                            quality=row.get("dist_quality", 1)
                        )
                    
                        print(card)
                        print(dataset_card_repository)
                        saved_card = dataset_card_repository.insert(card)
                    
                    saved_card = dataset_card_repository.get_by_dataset_id(row.get("card_dataset_id"))
                    if not saved_card:
                        saved_card= dataset_card_repository.get_by_name(row.get("card_dataset_name"))
                    # 2. Create and Insert Dataset
                    # Inherits the ID from the card just created
                    if not dataset_repository.exists_by_name(row.get("dist_dataset")):
                        dataset_obj = Dataset(
                            id=None,
                            uri= BASE_PREFIX + row.get("dist_uri"),
                            derived_card=saved_card.id,
                            name=row.get("dist_dataset"),
                            description=row.get("card_dataset_description"),
                            languages=row.get("card_languages", []),
                            license=row.get("card_license", "unknown"),
                            source=row.get("card_source_url"),
                            step=row.get("dist_step", 3)
                        )
                        saved_dataset = dataset_repository.insert(dataset_obj)
                    
                        # 3. Create and Insert Distribution
                        # Inherits the ID from the dataset just created
                        distribution = Distribution(
                            id=None, 
                            uri= BASE_PREFIX + row.get("dist_uri"),
                            tokenized_uri=None,
                            dataset_id=saved_dataset.id,
                            name= row.get("dist_name"),
                            glob="*.jsonl.gz", 
                            format="jsonl.gz",
                            lang=row.get("dist_lang", "un"),
                            step=row.get("dist_step", 3),
                            license=row.get("card_license", "unknown")
                        )
                        
                        saved_dist = distribution_repository.insert(distribution)
                        
                        # Log the completed triplet
                        log_msg = f"SUCCESS: Row {line_num} | Card: {saved_card.id} | Dataset: {saved_dataset.id} | Dist: {saved_dist.id}"
                        logger.info(log_msg)
                        
                        results.append({
                            "card": saved_card,
                            "dataset": saved_dataset,
                            "distribution": saved_dist
                        })

                except Exception as e:
                    logger.error(traceback.format_exc())
                    logger.error(f"Error inserting row {line_num}: {e}")
                    exit(0)

    except FileNotFoundError:
        logger.error(f"File not found at path: {file_path}")
    
    return results


_SCRIPT_DIR = Path(__file__).resolve().parent
input = str(_SCRIPT_DIR / "denormalized_view" / "unmatched_reviewed.jsonl")
triplette = process_and_insert_jsonl(input)
logger.info(f"Processing completed. Total triplets inserted: {len(triplette)}")

