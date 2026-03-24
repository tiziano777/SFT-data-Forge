import sys
sys.path.append("..")
from datatrove.executor import LocalPipelineExecutor
from reader.unified_reader import UnifiedReader
from stats.low_level_stats import DocStats

from typing import Dict, List, Any
MappingSpec = Dict[str, List[Any]]

import os


PROCESSED_DATA_DIR= os.getenv("PROCESSED_DATA_DIR")
STATS_DATA_DIR= os.getenv("STATS_DATA_DIR")
LOW_LEVEL_STATS_EXTENSION= os.getenv("LOW_LEVEL_STATS_EXTENSION")
def run_low_level_stats_pipeline():

    # PATHS:
    stats_path = STATS_DATA_DIR + "/velvet_v1/allenai/ARC-Challenge/en/"
    output_path= stats_path + LOW_LEVEL_STATS_EXTENSION

    processed_dataset_path = PROCESSED_DATA_DIR + "/velvet_v1/allenai/"
    processed_distribution_path = processed_dataset_path + "ARC-Challenge/en/"
    input_path= processed_distribution_path
    

    print("🚀 Starting Mapping Pipeline")
    print(f"Input path: {input_path}")
    print(f"Output path: {output_path}")

    reader= UnifiedReader(
        data_folder=processed_distribution_path,
        glob_pattern="*.jsonl.gz", # current glob of distribution processed data
        recursive=True,
        text_key="text",
        id_key="id",
        default_metadata={} 
    )
    low_level_stats = DocStats(
        output_folder=output_path,
    )


    pipeline= [reader,low_level_stats]
    
    pipe = LocalPipelineExecutor(
        pipeline=pipeline,
        tasks=1,
        logging_dir=None,
    )
    pipe.run()

if __name__ == "__main__":
    run_low_level_stats_pipeline()
