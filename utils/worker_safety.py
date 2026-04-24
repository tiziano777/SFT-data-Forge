"""
Wrapper di sicurezza per worker paralleli di preprocessing.

Monitora e cattura errori di serializzazione silentiosi che causano crash
senza controllare lo stderr durante il parallel processing.
"""

import logging
import sys
import os
from functools import wraps
from datetime import datetime, timezone

# Setup logging robusto per worker
def setup_worker_logging(worker_name: str = "worker", log_dir: str = "/tmp"):
    """Configura logging dedicato per il worker, con output sia su stderr che su file."""

    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    worker_id = os.getpid()
    log_file = os.path.join(log_dir, f"{worker_name}_{worker_id}_{timestamp}.log")

    # Crea logger
    logger = logging.getLogger(worker_name)
    logger.setLevel(logging.DEBUG)

    # File handler (CRITICAL - non viene perso su crash)
    try:
        os.makedirs(log_dir, exist_ok=True)
        fh = logging.FileHandler(log_file, encoding='utf-8')
        fh.setLevel(logging.DEBUG)
        logger.addHandler(fh)
    except Exception as e:
        print(f"⚠️ Impossibile creare log file: {e}", file=sys.stderr)

    # Stderr handler
    sh = logging.StreamHandler(sys.stderr)
    sh.setLevel(logging.ERROR)
    logger.addHandler(sh)

    # Formatter
    formatter = logging.Formatter(
        '[%(asctime)s - %(name)s - %(levelname)s] %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    for handler in logger.handlers:
        handler.setFormatter(formatter)

    logger.info(f"🔧 Worker logging setup: {log_file}")
    return logger, log_file


def safe_serialization_wrapper(func):
    """
    Wrapper che cattura e logga errori di serializzazione ricorsivi.
    Pensato per wrappare `json.dumps()` e simili.
    """
    @wraps(func)
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except TypeError as e:
            # Errore di serializzazione JSON - dettagli
            obj = args[0] if args else "unknown"
            raise TypeError(
                f"[SERIALIZATION_ERROR] Impossibile serializzare. "
                f"Object type: {type(obj).__name__}. "
                f"Original error: {e}"
            ) from e
        except Exception as e:
            raise

    return wrapper
