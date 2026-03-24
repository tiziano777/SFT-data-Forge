"""
Utility to map internal container paths to the binded host paths and back.
This uses environment variables set in the project 
(BASE_PREFIX, 
BASE_PATH, BINDED_BASE_PATH,
RAW_DATA_DIR, BINDED_RAW_DATA_DIR,
PROCESSED_DATA_DIR, BINDED_PROCESSED_DATA_DIR,
MAPPED_DATA_DIR, BINDED_MAPPED_DATA_DIR).
"""
import os
from pathlib import Path

BASE_PREFIX = os.getenv("BASE_PREFIX")

BASE_PATH = os.getenv("BASE_PATH")
BINDED_BASE_PATH = os.getenv("BINDED_BASE_PATH")

# Specific layer dirs (optional)
RAW_DATA_DIR = os.getenv("RAW_DATA_DIR")
BINDED_RAW_DATA_DIR = os.getenv("BINDED_RAW_DATA_DIR")
PROCESSED_DATA_DIR = os.getenv("PROCESSED_DATA_DIR")
BINDED_PROCESSED_DATA_DIR = os.getenv("BINDED_PROCESSED_DATA_DIR")
MAPPED_DATA_DIR = os.getenv("MAPPED_DATA_DIR")
BINDED_MAPPED_DATA_DIR = os.getenv("BINDED_MAPPED_DATA_DIR")

def _has_prefix(path: str) -> bool:
    return path.startswith(BASE_PREFIX)

def _strip_prefix(path: str) -> str:
    if _has_prefix(path):
        return path[len(BASE_PREFIX):]
    return path

def _add_prefix(path: str, original: str) -> str:
    return f"{BASE_PREFIX}{path}" if _has_prefix(original) else path

def _layer_replace(path: str, src: str, dst: str) -> str:
    if not src or not dst:
        return None
    if path.startswith(src):
        return path.replace(src, dst, 1)
    return None

def to_binded_path(path: str) -> str:
    """Convert an internal/container path to its binded host path.
    If the path has the BASE_PREFIX (e.g. file://) it is preserved.
    """
    if not path:
        return path

    # Convert path to string if it's a PosixPath
    if isinstance(path, Path):
        path = str(path)

    original = path
    p = _strip_prefix(path)

    # Try specific layer dirs first
    for src, dst in ((RAW_DATA_DIR, BINDED_RAW_DATA_DIR),
                     (PROCESSED_DATA_DIR, BINDED_PROCESSED_DATA_DIR),
                     (MAPPED_DATA_DIR, BINDED_MAPPED_DATA_DIR)):
        r = _layer_replace(p, src, dst)
        if r:
            return _add_prefix(r, original)

    # General base path replacement
    if BASE_PATH and BINDED_BASE_PATH and p.startswith(BASE_PATH):
        return _add_prefix(p.replace(BASE_PATH, BINDED_BASE_PATH, 1), original)

    # Fallback: if BASE_PATH appears anywhere
    if BASE_PATH and BINDED_BASE_PATH and BASE_PATH in p:
        return _add_prefix(p.replace(BASE_PATH, BINDED_BASE_PATH), original)

    return _add_prefix(p, original)

def to_internal_path(path: str) -> str:
    """Convert a binded host path back to internal/container path.
    Preserves the BASE_PREFIX if present in the original value.
    """
    if not path:
        return path
    original = path
    p = _strip_prefix(path)

    # Try specific layer dirs first (reverse)
    for src, dst in ((BINDED_RAW_DATA_DIR, RAW_DATA_DIR),
                     (BINDED_PROCESSED_DATA_DIR, PROCESSED_DATA_DIR),
                     (BINDED_MAPPED_DATA_DIR, MAPPED_DATA_DIR)):
        if src and dst and p.startswith(src):
            return _add_prefix(p.replace(src, dst, 1), original)

    # General base path replacement (reverse)
    if BINDED_BASE_PATH and BASE_PATH and p.startswith(BINDED_BASE_PATH):
        return _add_prefix(p.replace(BINDED_BASE_PATH, BASE_PATH, 1), original)

    if BINDED_BASE_PATH and BASE_PATH and BINDED_BASE_PATH in p:
        return _add_prefix(p.replace(BINDED_BASE_PATH, BASE_PATH), original)

    return _add_prefix(p, original)

