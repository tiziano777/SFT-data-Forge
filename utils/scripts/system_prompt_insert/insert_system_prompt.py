import json
import os
import dotenv
from pathlib import Path

_PROJECT_ROOT = Path(__file__).resolve().parents[3]
dotenv.load_dotenv(str(_PROJECT_ROOT / "docker" / "dev" / ".env.dev"))  # Load environment variables from .env file
import logging
from db.impl.postgres.loader.postgres_db_loader import get_db_manager
from data_class.repository.table.system_prompt_repository import SystemPromptRepository
from data_class.entity.table.system_prompt import SystemPrompt

# Logging configuration
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def load_from_jsonl(file_path: str, repo: SystemPromptRepository):
    if not os.path.exists(file_path):
        logger.error(f"File not found: {file_path}")
        return

    with open(file_path, 'r', encoding='utf-8') as f:
        count_new = 0
        count_updated = 0

        for line in f:
            if not line.strip():
                continue

            data = json.loads(line)

            # Map from JSONL to Dataclass
            # Note: id is left as None because the DB generates it (UUID)
            new_prompt = SystemPrompt(
                id=None,
                name=data['name'],
                description=data['description'],
                prompt=data['prompt'],
                length=data['_length'],
                _lang=data.get('_lang', 'un'),
                quality_score=0.5,
                deleted=False,
                version='1.0'
            )

            # Check existence to handle insert or update
            existing = repo.get_by_name(new_prompt.name)

            try:
                if existing:
                    # Update the necessary fields on the existing object
                    existing.prompt = new_prompt.prompt
                    existing.length = new_prompt.length
                    existing.description = new_prompt.description
                    existing._lang = new_prompt._lang
                    repo.update(existing)
                    count_updated += 1
                else:
                    repo.insert(new_prompt)
                    count_new += 1
            except Exception as e:
                logger.error(f"Error processing {new_prompt.name}: {e}")

    logger.info(f"Process completed: {count_new} new inserted, {count_updated} updated.")

if __name__ == "__main__":
    db_manager = get_db_manager() 
    prompt_repo = SystemPromptRepository(db_manager)
    
    JSONL_FILE = "prompts_dataset.jsonl"
    load_from_jsonl(JSONL_FILE, prompt_repo)