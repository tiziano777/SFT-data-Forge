"""
Migration script: normalizes the noisy parquet file to the dataset_card DB schema.
PART 1: general field cleanup
PART 2: normalization and matching of core_skills and tasks vs DB vocabulary
"""

import re
import logging
import pandas as pd
from difflib import SequenceMatcher

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger(__name__)

# -- Separate loggers for audit --
acquired_logger  = logging.getLogger("acquired")
discarded_logger = logging.getLogger("discarded")

acquired_handler  = logging.FileHandler("skills_tasks_acquired.log",  encoding="utf-8")
discarded_handler = logging.FileHandler("skills_tasks_discarded.log", encoding="utf-8")
acquired_handler.setFormatter(logging.Formatter("%(asctime)s %(message)s"))
discarded_handler.setFormatter(logging.Formatter("%(asctime)s %(message)s"))
acquired_logger.addHandler(acquired_handler);   acquired_logger.setLevel(logging.INFO)
discarded_logger.addHandler(discarded_handler); discarded_logger.setLevel(logging.INFO)

from pathlib import Path

_SCRIPTS_DIR = Path(__file__).resolve().parents[1]
card_input_file_path  = str(_SCRIPTS_DIR / "velvet___b_dsj_datasets_seeds.parquet")
card_output_file_path = str(_SCRIPTS_DIR / "dataset_cards.parquet")

SIMILARITY_THRESHOLD = 0.65


# ======================================================================
# NORMALIZATION UTILITY for ID and Name
# ======================================================================

def custom_normalize(text):
    """
    1. Lowercase
    2. Spaces -> '__' (double underscore)
    3. Keeps: alphanumerics, underscores (_), hyphens (-) and dots (.)
    """
    if not isinstance(text, str) or pd.isna(text):
        return ""

    # 1. Lowercase and strip
    s = text.lower().strip()

    # 2. Replace spaces with double underscore
    s = re.sub(r'\s+', '__', s)

    # 3. Regex: keep a-z, 0-9, underscore, hyphen and dot
    # Note: dot and hyphen are escaped with \
    s = re.sub(r'[^a-z0-9_\-\.]', '', s)

    # Final cleanup of any remaining symbols at edges
    return s.strip('_').strip('-').strip('.')
# ══════════════════════════════════════════════════════════════════════════════
# PARTE 1 — pulizia generale
# ══════════════════════════════════════════════════════════════════════════════

df = pd.read_parquet(card_input_file_path)
logger.info(f"Righe iniziali: {len(df)}")

# 1. Drop colonne inutili
drop_cols = ["_row_number", "_row_dsj", "leaderboard", "benchmark", "split", "type"]
df.drop(columns=[c for c in drop_cols if c in df.columns], inplace=True)

# 2. Rinomina colonne rumorose → nomi DB
rename_map = {
    "dataset name":            "dataset_name",
    "Reasoning field":         "Reasoning field",
    "licence":                 "license",
    "core skill / capability": "core_skills",
    "tasks":                   "tasks",
}
df.rename(columns=rename_map, inplace=True)

# --- NUOVA LOGICA DI NORMALIZZAZIONE ---
# 3. Normalizzazione dataset_id e dataset_name
logger.info("Normalizzazione dataset_id e dataset_name...")
df["dataset_id"]   = df["dataset_id"].apply(custom_normalize)
df["dataset_name"] = df["dataset_name"].apply(custom_normalize)

# 4. Drop righe senza dataset_id o dataset_name (dopo normalizzazione)
before = len(df)
df = df[df["dataset_id"] != ""]
df = df[df["dataset_name"] != ""]
logger.info(f"Righe dopo drop missing/empty id/name: {len(df)} (rimosse {before - len(df)})")

# 5. Dedup su dataset_id
before = len(df)
df.drop_duplicates(subset=["dataset_id"], keep="first", inplace=True)
logger.info(f"Righe dopo dedup dataset_id: {len(df)} (rimosse {before - len(df)})")

# 6. has_reasoning: null → False, qualsiasi valore → True
reasoning_col = "Reasoning field"
if reasoning_col in df.columns:
    df["has_reasoning"] = df[reasoning_col].apply(
        lambda x: False if (
            x is None or (isinstance(x, float) and pd.isna(x)) or str(x).strip() == ""
        ) else True
    )
    df.drop(columns=[reasoning_col], inplace=True)
else:
    df["has_reasoning"] = False

# 7. Languages → array normalizzato
VALID_LANG_RE = re.compile(r'^[a-z]{2}$')

def normalize_languages(raw) -> list:
    if raw is None or (isinstance(raw, float) and pd.isna(raw)) or str(raw).strip() == "":
        return []
    result = []
    for item in str(raw).split(","):
        n = item.lower().strip()
        if (n == "multi" or VALID_LANG_RE.match(n)) and n not in result:
            result.append(n)
    return result

df["languages"] = df["languages"].apply(normalize_languages)

# 8. Default campi stringa
def coerce_str(val, default="") -> str:
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return default
    s = str(val).strip()
    return s if s else default

df["license"]    = df["license"].apply(lambda x: coerce_str(x, "unknown"))
df["source_url"] = df["source_url"].apply(lambda x: coerce_str(x, ""))
df["notes"]      = df["notes"].apply(lambda x: coerce_str(x, ""))

# 9. Campi fissi non presenti nel parquet
df["modality"]            = "text"
df["dataset_description"] = None
df["publisher"]           = None
df["download_url"]        = ""
df["source_type"]         = "unknown"

# 10. Parse iniziale core_skills e tasks come liste grezze (matching ancora da fare)
def raw_to_list(val) -> list:
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return []
    if isinstance(val, list):
        return val
    result= [v.strip() for v in str(val).split(",") if v.strip()]
    return result[:1]

df["core_skills"] = df["core_skills"].apply(raw_to_list)
df["tasks"]       = df["tasks"].apply(raw_to_list)

# 11. Colonne extra DB con default lista vuota
for col in ["sources", "fields", "vertical", "contents"]:
    if col not in df.columns:
        df[col] = [[] for _ in range(len(df))]


# ══════════════════════════════════════════════════════════════════════════════
# PARTE 2 — normalizzazione e matching core_skills / tasks vs vocabolario DB
# ══════════════════════════════════════════════════════════════════════════════

import os
from db.impl.postgres.postgres_db_manager import PostgresDBManager
from data_class.repository.vocabulary.vocab_core_skill_repository import VocabCoreSkillRepository
from data_class.repository.vocabulary.skill_task_taxonomy_repository import SkillTaskTaxonomyRepository
from data_class.repository.vocabulary.vocab_license_repository import VocabLicenseRepository

POSTGRES_DB_SCHEMA = os.getenv("DB_SCHEMA") or "MLdatasets"
def get_db_manager() -> PostgresDBManager:
    """Restituisce un'istanza di PostgresDBManager con parametri da variabili d'ambiente."""
    dbname = os.getenv("POSTGRES_DB","postgres")
    user = os.getenv("POSTGRES_USER", "T.Finizzi")
    password = os.getenv("POSTGRES_PASSWORD", "postgres")
    host = os.getenv("POSTGRES_HOST", "postgres")
    port = int(os.getenv("POSTGRES_PORT", 5432))

    if not all([dbname, user, password]):
        raise ValueError("⚠️ Variabili d'ambiente DB mancanti: POSTGRES_DB, POSTGRES_USER, POSTGRES_PASSWORD")
    db = PostgresDBManager(dbname, user, password, host, port)
    db.connect()
    return db

db_manager        = get_db_manager()
vocab_skill_repo  = VocabCoreSkillRepository(db_manager)
taxonomy_repo     = SkillTaskTaxonomyRepository(db_manager)
license_repo      = VocabLicenseRepository(db_manager)
licenses = license_repo.get_all()
licenses= [l.code for l in licenses]


# Carica vocabolario skills dal DB
vocab_skill_codes: list[str] = [v.code for v in vocab_skill_repo.get_all()]
logger.info(f"Vocabolario core_skills caricato: {len(vocab_skill_codes)} voci")

# Cache task ammissibili per skill (evita N query per ogni riga)
_task_cache: dict[str, list[str]] = {}

def get_tasks_for_skill(skill_code: str) -> list[str]:
    """Ritorna i task_code ammissibili per una skill_code (con cache)."""
    if skill_code in _task_cache:
        return _task_cache[skill_code]
    table = f"{POSTGRES_DB_SCHEMA}.skill_task_taxonomy"
    query = f"SELECT task_code FROM {table} WHERE skill_code = %s"
    with db_manager as db:
        rows = db.query(query, (skill_code,))
        result = [r["task_code"] for r in rows] if rows else []
    _task_cache[skill_code] = result
    return result

# ── Utility: normalizza raw string → code format (^[a-z_]+$) ─────────────────
VALID_CODE_RE = re.compile(r'^[a-z_]+$')

def normalize_to_code(raw: str) -> str | None:
    """
    Normalizza in lowercase + underscore.
    Ritorna None se il risultato non soddisfa ^[a-z_]+$.
    """
    s = raw.lower().strip()
    s = re.sub(r'[\s\-/]+', '_', s)    # spazi, trattini, slash → underscore
    s = re.sub(r'[^a-z_]', '', s)      # rimuovi tutto il resto
    s = re.sub(r'_+', '_', s).strip('_')
    return s if s and VALID_CODE_RE.match(s) else None

# ── Utility: similarity ratio ─────────────────────────────────────────────────
def similarity(a: str, b: str) -> float:
    return SequenceMatcher(None, a, b).ratio()

# ── Utility: best match in vocabolario ───────────────────────────────────────
def best_match(code: str, vocab: list[str]) -> tuple[str | None, float]:
    """Ritorna (best_code, score). best_code è None se score < SIMILARITY_THRESHOLD."""
    best_code, best_score = None, 0.0
    for v in vocab:
        s = similarity(code, v)
        if s > best_score:
            best_score, best_code = s, v
    if best_score >= SIMILARITY_THRESHOLD:
        return best_code, best_score
    return None, best_score

# ── Processing core_skills per una card ──────────────────────────────────────
def process_core_skills(raw_list: list, dataset_id: str) -> list[str]:
    result = []
    if raw_list is None:
        return result
    for raw in raw_list:
        code = normalize_to_code(raw)

        if code is None:
            discarded_logger.info(
                f"[core_skill] dataset_id={dataset_id!r} raw={raw!r} "
                f"reason=failed_normalization"
            )
            continue

        if code in vocab_skill_codes:
            acquired_logger.info(
                f"[core_skill] dataset_id={dataset_id!r} raw={raw!r} "
                f"code={code!r} reason=exact_match"
            )
            result.append(code)
        else:
            matched, score = best_match(code, vocab_skill_codes)
            if matched:
                acquired_logger.info(
                    f"[core_skill] dataset_id={dataset_id!r} raw={raw!r} "
                    f"code={matched!r} reason=similar_match score={score:.3f}"
                )
                result.append(matched)
            else:
                discarded_logger.info(
                    f"[core_skill] dataset_id={dataset_id!r} raw={raw!r} "
                    f"normalized={code!r} reason=no_match best_score={score:.3f}"
                )

    # Dedup mantenendo ordine
    seen: set = set()
    return [x for x in result if not (x in seen or seen.add(x))]

# ── Processing tasks per una card ─────────────────────────────────────────────
def process_tasks(raw_task_list: list, validated_skills: list[str], dataset_id: str) -> list[str]:
    # Unione task ammissibili per tutte le skill validate della card
    admissible: list[str] = []
    for skill in validated_skills:
        for t in get_tasks_for_skill(skill):
            if t not in admissible:
                admissible.append(t)

    result = []
    for raw in raw_task_list:
        code = normalize_to_code(raw)

        if code is None:
            discarded_logger.info(
                f"[task] dataset_id={dataset_id!r} raw={raw!r} "
                f"reason=failed_normalization"
            )
            continue

        if not admissible:
            discarded_logger.info(
                f"[task] dataset_id={dataset_id!r} raw={raw!r} "
                f"normalized={code!r} reason=no_admissible_tasks_for_skills"
            )
            continue

        if code in admissible:
            acquired_logger.info(
                f"[task] dataset_id={dataset_id!r} raw={raw!r} "
                f"code={code!r} reason=exact_match"
            )
            result.append(code)
        else:
            matched, score = best_match(code, admissible)
            if matched:
                acquired_logger.info(
                    f"[task] dataset_id={dataset_id!r} raw={raw!r} "
                    f"code={matched!r} reason=similar_match score={score:.3f}"
                )
                result.append(matched)
            else:
                discarded_logger.info(
                    f"[task] dataset_id={dataset_id!r} raw={raw!r} "
                    f"normalized={code!r} reason=no_match best_score={score:.3f}"
                )

    # Dedup mantenendo ordine
    seen: set = set()
    return [x for x in result if not (x in seen or seen.add(x))]

# ── Applica a tutto il dataframe ──────────────────────────────────────────────
logger.info("Inizio processing core_skills e tasks...")

validated_skills_col: list = []
validated_tasks_col:  list = []
validated_licenses_col: list = []

for  row in df.itertuples(index=False):
    did    = row.dataset_id
    skills = process_core_skills(row.core_skills, did)
    tasks  = process_tasks(row.tasks, skills, did)
    validated_skills_col.append(skills)
    validated_tasks_col.append(tasks)
    if row.license not in licenses:
        validated_licenses_col.append("unknown")
    else:
        validated_licenses_col.append(row.license)

df["core_skills"] = validated_skills_col
df["tasks"]       = validated_tasks_col
df["license"]     = validated_licenses_col

logger.info("Processing core_skills e tasks completato.")


# ══════════════════════════════════════════════════════════════════════════════
# FINALE — selezione colonne e salvataggio
# ══════════════════════════════════════════════════════════════════════════════

final_cols = [
    "dataset_name", "dataset_id", "modality", "dataset_description",
    "publisher", "notes", "source_url", "download_url", "languages",
    "license", "core_skills", "tasks", "sources", "source_type",
    "fields", "vertical", "contents", "has_reasoning",
]
df = df[[c for c in final_cols if c in df.columns]]

df.to_parquet(card_output_file_path, index=False)
logger.info(f"File salvato in: {card_output_file_path}")
logger.info(f"Righe finali: {len(df)}")
print(df.head())