import os
import sys
import multiprocessing


from datatrove_pipelines.low_level_stats_pipeline.reader.unified_reader import UnifiedReader
from datatrove_pipelines.low_level_stats_pipeline.stats.low_level_stats import DocStats
from datatrove.executor import LocalPipelineExecutor 



PROCESSED_DATA_DIR = os.getenv("PROCESSED_DATA_DIR")
STATS_DATA_DIR = os.getenv("STATS_DATA_DIR")
LOW_LEVEL_STATS_EXTENSION = os.getenv("LOW_LEVEL_STATS_EXTENSION")


_KNOWN_GLOBS = [
    ("**/*.jsonl.gz", "jsonl.gz"),
    ("**/*.parquet", "parquet"),
    ("**/*.jsonl", "jsonl"),
    ("**/*.json", "json"),
]


def _detect_glob(path: str) -> str:
    from pathlib import Path
    for pattern, _ in _KNOWN_GLOBS:
        if list(Path(path).glob(pattern)):
            print(f"[stats_worker] Formato rilevato: {pattern}", flush=True)
            return pattern
    raise RuntimeError(f"Nessun file supportato trovato in {path}")


def run_low_level_pipeline(distribution_uri: str):
    input_path = distribution_uri
    output_path = (
        distribution_uri
        .replace(PROCESSED_DATA_DIR, STATS_DATA_DIR)
        + LOW_LEVEL_STATS_EXTENSION
    )

    os.makedirs(output_path, exist_ok=True)

    glob_pattern = _detect_glob(input_path)
    n_cores = multiprocessing.cpu_count()

    reader = UnifiedReader(
        data_folder=input_path,
        glob_pattern=glob_pattern,
        recursive=True,
        text_key="text",
        id_key="id",
        default_metadata={}
    )

    low_level_stats = DocStats(output_folder=output_path)

    pipeline = [reader, low_level_stats]

    pipe = LocalPipelineExecutor(
        pipeline=pipeline,
        tasks=n_cores * 10,
        workers=int(n_cores * 0.85),
        logging_dir=None
    )

    pipe.run()
    print("Estrazione statistiche completata.")


if __name__ == "__main__":
    import traceback
    print(f"[stats_worker] START argv={sys.argv}", flush=True)
    print(f"[stats_worker] ENV PROCESSED_DATA_DIR={PROCESSED_DATA_DIR} STATS_DATA_DIR={STATS_DATA_DIR} LOW_LEVEL_STATS_EXTENSION={LOW_LEVEL_STATS_EXTENSION}", flush=True)
    if len(sys.argv) > 1:
        try:
            run_low_level_pipeline(sys.argv[1])
        except Exception as e:
            print(f"[stats_worker] FATAL ERROR: {e}", flush=True)
            traceback.print_exc()
            sys.exit(1)
    else:
        print("[stats_worker] Nessun argomento passato", flush=True)
