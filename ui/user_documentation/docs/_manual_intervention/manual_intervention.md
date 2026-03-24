# 10. Manual FileSystem Intervention & Stats Integrity

Guide to manual interaction with the storage silos, path routing logic, and advanced analysis management through the statistics silo.

---

## Data Architecture (3-Layer Logic)

The SFT Data Forge system organizes data into physically separate silos on disk, corresponding to the logical layers of the data lifecycle. Every manual intervention must rigorously respect the path hierarchy to be correctly indexed by the Metadata DB.

### The Path Formula

Every resource in the system is identified by a URI constructed according to the following logic in the DB:

```
PREFIX + LAYER_BASE_PATH + DATASET_PATH + DISTRIBUTION_PATH
```

For statistics files of a dataset:

```
PREFIX + LAYER_STATS_PATH + DATASET_PATH + DISTRIBUTION_PATH + TYPE_STATS_EXTENSION
```

Where:

* **PREFIX**: Usually `file://` or `nfs://` (identifies the disk type/name).
* **LAYER_BASE_PATH**: The root directory of the layer (e.g., `/nfs/data-download`).
* **DATASET_PATH**: The root folder of the dataset or the path, in case it was pre-grouped.
* **DISTRIBUTION_PATH**: The specific path of the data branch down to the leaf folder containing only files.
* **TYPE_STATS_EXTENSION**: An additional branching level used to subdivide statistics tables by groups and reference schemas (e.g., `low_level_stats`, `chat_template_stats`, `task_classification_stats`...).

---

## Directory Mapping (Environment Reference)

Based on the system configuration, the main mount points are saved in the configuration file under these names:

```bash
# LAYER 1 - RAW DATA Global mapping for physical disk
BINDED_RAW_DATA_DIR=...
# LAYER 2 - PROCESSED DATA physical disk
BINDED_PROCESSED_DATA_DIR=...
# LAYER 3 - MAPPED DATA physical disk
BINDED_MAPPED_DATA_DIR=...
# STATS DATA PATHS
BINDED_STATS_DATA_DIR=...
# DEFAULT TYPE STATS EXTENSION
LOW_LEVEL_STATS_EXTENSION=/low_level_stats
CHAT_TEMPLATE_STATS_EXTENSION=/chat_template_stats
```

---

## Manual Insertion Constraints

To manually insert a dataset on disk, choose the layer based on the **maturity of the data**:

1. **Insertion in RAW**: The data is in its original format, without technical metadata (`_id_hash`, `_lang`,...). Requires the Wizard from the Dashboard for registration.
2. **Insertion in PROCESSED**: The data is already standardized. Must contain the **Core Metadata** (`_id_hash`, `_lang`, `_subpath`, `_filename`, `_dataset_name`, `_dataset_path`) in every record.
3. **Insertion in MAPPED**: The data is ready for training and perfectly adheres to a saved **Schema Template**. Insert here only if no further mappings from one schema to another are intended.

---

## Ontological Validation Triggers

When inserting entities manually (via direct DB insert), the following database triggers are enforced:

* **Step Lineage Validation** (`fn_validate_step_lineage`): A derived entity cannot have a step value lower than its parent. For example, you cannot insert a PROCESSED dataset (step 2) that claims to derive from a MAPPED dataset (step 3).
* **Distribution-Dataset Alignment** (`trg_distribution_logic_check`): A distribution's step must always match the step of its parent dataset. Additionally, if the distribution has a `derived_from` reference, the source distribution's step must be <= the current distribution's step.
* **Strategy Eligibility** (`trg_validate_strategy_eligibility`): Only distributions with step = 3 (MAPPED) can be used in recipe strategies.

These triggers cannot be bypassed and will reject any INSERT or UPDATE that violates the ontological rules.

---

## Advanced Stats Silo: Mirroring Logic

The Statistics Silo is designed to host data enrichments without modifying files in the primary layers. It follows a **mirroring** logic of the original path:

* **Original Path**: `[LAYER]_BASE_PATH` + `/dataset_path/distribution_path/`
* **Stats Path**: `STATS_DATA_DIR` + `/dataset_path/distribution_path/` + `[TYPE]_STATS_EXTENSION`

This mirroring allows the Dashboard SQL to automatically attach the Parquet count tables to the source data.

> **NOTE:** Currently only two statistics tables are available and configured. They are extensible, but if you want to create a new table, the SQL interface section must be reconfigured to unlock new features. However, concatenating existing stats with manually computed ones works, resulting in a coherent denormalized view.

---

## Extensibility and Custom Columns

The stats silo allows adding arbitrary columns derived from external analyses (e.g., `cluster_id`, `toxicity_score`, `sentiment_label`).

> **WARNING:** **Join Constraint**: Every file in the statistics silo must contain an identifying column that references the `_id_hash` field of the original record via the `id` field, otherwise the system join will break.

---

## Deterministic ID Generation (`_id_hash`)

To ensure integrity between the layers and the stats silo, SFT Data Forge uses a deterministic text extraction function to generate hashes.

### Extraction Function (Reference)

For external scripts, use this logic to ensure ID coherence:

```python
import hashlib

# AUX Function
def _extract_text_from_data(data_dict: dict) -> str:
    """
    Extracts text from a dictionary in a deterministic way.
    Sorts keys alphanumerically to ensure the hash result
    is consistent regardless of field order.
    """
    if not data_dict:
        return ""

    # 1. Get keys and sort them alphanumerically
    sorted_keys = sorted(data_dict.keys())

    parts = []
    for key in sorted_keys:
        value = data_dict[key]
        # 2. Skip None values to avoid hash variations
        if value is None:
            continue

        # 3. Convert to string and concatenate
        parts.append(str(value))

    return "".join(parts)


# MAIN Function
def _compute_id_hash(text: str) -> str:
    """Computes the SHA256 hash of the text to generate the ID."""
    if not text:
        raise ValueError("The text for hash computation is empty.")
    hash_obj = hashlib.sha256(text.encode('utf-8'))
    return hash_obj.hexdigest()
```

---

## Manual Enrichment Workflow

1. **Analysis**: Execute the external analysis script on a distribution.
2. **Save**: Generate a Parquet file containing a 1:1 mapping (`src.id` : `distribution._id_hash`) and the new analysis columns.
3. **Upload**: Copy the file to the statistics silo respecting the mirrored path:
    ```
    /nfs/stats-data/{dataset_path}/{distribution_path}/{[TYPE]_STATS_EXTENSION}/{name}_{rank}.parquet
    ```
4. **Query**: Use the Dashboard SQL to query the original data in JOIN with the newly loaded statistics.

---

## Distribution Entity for Manual Derivation

```python
class Distribution:
    id: str
    uri: str
    dataset_id: str
    glob: str
    format: str
    name: str
    query: Optional[str] = None
    script: Optional[str] = None
    lang: str = 'un'
    split: Optional[str] = None
    derived_from: Optional[str] = None
    src_schema: Dict[str, Any] = None
    description: Optional[str] = None
    tags: List[str] = None
    license: str = 'unknown'
    version: str = '1.0'
    issued: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    modified: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    materialized: bool = True
    step: Optional[int] = None
```

## Dataset Entity for Distribution Proliferation

```python
class Dataset:
    id: Optional[str] = None
    uri: str = ''
    name: str = ''
    languages: List[str] = field(default_factory=list)
    derived_card: Optional[str] = None
    derived_dataset: Optional[str] = None
    dataset_type: Optional[str] = None
    globs: List[str] = field(default_factory=list)
    description: Optional[str] = None
    source: Optional[str] = None
    version: str = '1.0'
    issued: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    modified: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    license: str = 'unknown'
    step: Optional[int] = None
```

## Card Composition for Manual Derivation

When manually creating parent/child card relationships, use the `card_composition` table:

* **Fields**: `parent_card_name` (TEXT), `child_card_name` (TEXT), `weight` (NUMERIC 0.00-1.00, optional).
* **Constraints**: No self-reference allowed. Both card names must exist in the `dataset_card` table.
* **Cascade**: Renaming a card automatically updates the composition. Deleting a card removes its composition rows.

---

## Download Python Example Code for Safe Manual Intervention

[DOWNLOAD_TOOLKIT_PYTHON]
