import duckdb
import pandas as pd

def read_parquet(file_path):
    """Reads a parquet file and returns a dataframe"""
    return pd.read_parquet(file_path)

def join_datasets(dist_path, card_path, output_dir="./"):
    """
    Performs partial join between dataset_distribution and dataset_card.
    Condition: dataset_card.dataset_id must be contained in dataset_distribution.dataset_id
    """
    # Read files
    print("Reading files...")
    dist_df = read_parquet(dist_path)
    card_df = read_parquet(card_path)
    
    print(f"   Distribution: {len(dist_df)} record")
    print(f"   Card: {len(card_df)} record")
    
    # DuckDB connection
    conn = duckdb.connect()
    conn.register('dist', dist_df)
    conn.register('card', card_df)
    
    # Query join: card.dataset_id must be contained in dist.dataset_id
    query = """
    SELECT 
        dist.dataset_id as dist_dataset_id,
        dist.name as dist_name,
        dist.quality as dist_quality,
        dist.uri as dist_uri,
        dist.step as dist_step,
        dist.lang as dist_lang,
        dist.dataset as dist_dataset,
        
        card.dataset_id as card_dataset_id,
        card.dataset_name as card_dataset_name,
        card.modality as card_modality,
        card.dataset_description as card_dataset_description,
        card.publisher as card_publisher,
        card.notes as card_notes,
        card.source_url as card_source_url,
        card.download_url as card_download_url,
        card.languages as card_languages,
        card.license as card_license,
        card.core_skills as card_core_skills,
        card.tasks as card_tasks,
        card.sources as card_sources,
        card.source_type as card_source_type,
        card.fields as card_fields,
        card.vertical as card_vertical,
        card.contents as card_contents,
        card.has_reasoning as card_has_reasoning
        
    FROM dist
    LEFT JOIN card 
        ON POSITION(card.dataset_id IN dist.dataset_id) > 0
    """
    
    print("Executing join...")
    result_df = conn.execute(query).df()

    # Separate matched and unmatched
    matched = result_df[result_df['card_dataset_id'].notna()].copy()
    unmatched = result_df[result_df['card_dataset_id'].isna()].copy()
    
    # Save JSONL
    print("Saving...")
    
    matched.to_json(f"{output_dir}/matched.jsonl", orient='records', lines=True, force_ascii=False)
    unmatched.to_json(f"{output_dir}/unmatched.jsonl", orient='records', lines=True, force_ascii=False)
    
    print(f"Done! Matched: {len(matched)}, Unmatched: {len(unmatched)}")
    
    return matched, unmatched

# Usage
from pathlib import Path
_PROJECT_ROOT = Path(__file__).resolve().parents[3]
dist_path = str(_PROJECT_ROOT / "nfs/data-download/velvet_v1/velvet_v3_25B_kb/sft_to_distribution/mapped_velvet___b_expj_datasets_finetuning.parquet")
card_path = str(_PROJECT_ROOT / "nfs/data-download/velvet_v1/velvet_v3_25B_kb/seed_to_card/dataset_cards.parquet")
output_dir = str(_PROJECT_ROOT / "nfs/data-download/velvet_v1/velvet_v3_25B_kb/denormalized_view/")
matched, unmatched = join_datasets(dist_path, card_path, output_dir)

