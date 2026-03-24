# 5. Dataset & Distribution Workflow

Guided disk navigation and progressive dataset management through the processing layers (`RAW`, `PROCESSED`, `MAPPED`).

---

## FileSystem Navigation

In SFT Data Forge, the **Dataset** acts as a logical and physical "root" that aggregates multiple **Distributions**. While the Dataset contains high-level metadata (overall languages, dependencies, group tags), Distributions represent the actual physical branches (split, context, phase).

Users can navigate these levels through three dedicated workflows in the sidebar:

1. **RAW Dataset Workflow**: For acquiring and managing raw data.
2. **PROCESSED Dataset Workflow**: For managing derived and transformed data.
3. **MAPPED Dataset Workflow**: For data ready in standardized format.

In each layer it is possible to **register a new dataset** (via a guided Wizard) or **edit an existing dataset** (via a metadata editing form).

---

## Dataset Metadata Fields

Each **Dataset** entity is described by the following fields:

| Field             | Description                                                      | Required     |
|-------------------|------------------------------------------------------------------|--------------|
| `id`              | UUID primary key, auto-generated.                                | Yes (auto)   |
| `uri`             | Unique URI of the dataset (must match `protocol://` format).     | Yes          |
| `name`            | Unique human-readable name of the dataset.                       | Yes          |
| `dataset_type`    | Type of dataset (e.g., training, test). From `vocab_dataset_type`. | No         |
| `globs`           | Glob patterns for associated files.                              | Yes (auto)   |
| `languages`       | Languages of the dataset. Must contain at least one element.     | Yes          |
| `description`     | Free-text description of the dataset.                            | No           |
| `source`          | Source URI of the dataset.                                       | No           |
| `license`         | License of the dataset. Default: `unknown`.                      | Yes (auto)   |
| `derived_card`    | FK to `dataset_card.id` -- the logical card this dataset belongs to. | Yes      |
| `derived_dataset` | FK to `dataset.id` -- parent dataset for cross-layer lineage.    | No           |
| `version`         | Semantic version (e.g., `1.0`). Auto-incremented on update.     | Yes (auto)   |
| `issued`          | Timestamp of entity creation (automatic).                        | Yes (auto)   |
| `modified`        | Timestamp of last modification (automatic).                      | Yes (auto)   |
| `step`            | Layer indicator: 1=RAW, 2=PROCESSED, 3=MAPPED. Auto-detected from URI. | Yes (auto) |

---

## Distribution Metadata Fields

Each **Distribution** entity is described by the following fields:

| Field             | Description                                                      | Required     |
|-------------------|------------------------------------------------------------------|--------------|
| `id`              | UUID primary key, auto-generated.                                | Yes (auto)   |
| `uri`             | Unique URI of the distribution (must match `protocol://` format). | Yes         |
| `dataset_id`      | FK to `dataset.id` -- the parent dataset.                        | Yes          |
| `name`            | Unique human-readable name.                                      | Yes          |
| `glob`            | Glob pattern for the distribution files.                         | Yes          |
| `format`          | File format (e.g., `jsonl`, `parquet`).                          | Yes          |
| `query`           | SQL query used to derive this distribution (if applicable).      | No           |
| `script`          | Script reference used for derivation (if applicable).            | No           |
| `derived_from`    | FK to `distribution.id` -- source distribution for lineage.      | No           |
| `src_schema`      | Extracted source schema (JSONB). Populated by Schema Extractor.  | No           |
| `description`     | Free-text description.                                           | No           |
| `lang`            | ISO language code. Default: `un` (undetermined).                 | Yes (auto)   |
| `split`           | Dataset split type (e.g., `train`, `test`). From `vocab_split`.  | No           |
| `materialized`    | Whether the distribution has physical files on disk.             | Yes (auto)   |
| `tags`            | Free-form tags for classification.                               | No           |
| `license`         | License. Default: `unknown`.                                     | Yes (auto)   |
| `version`         | Semantic version. Auto-incremented on update.                    | Yes (auto)   |
| `issued`          | Timestamp of creation (automatic).                               | Yes (auto)   |
| `modified`        | Timestamp of last modification (automatic).                      | Yes (auto)   |
| `step`            | Layer indicator: 1=RAW, 2=PROCESSED, 3=MAPPED. Auto-detected.   | Yes (auto)   |

---

## Step & Lifecycle

The `step` field determines which physical layer an entity belongs to:

| Step | Layer     | Description                                              |
|------|-----------|----------------------------------------------------------|
| 1    | RAW       | Raw data in original format, no metadata enrichment.     |
| 2    | PROCESSED | Standardized data with core metadata fields appended.    |
| 3    | MAPPED    | Data mapped to an enterprise schema template, training-ready. |

### Automatic Step Detection

The `step` is **automatically assigned** based on the entity's URI. The `config_paths` table maps filesystem path prefixes to step values. A database trigger (`fn_update_step_from_uri`) fires on every INSERT or UPDATE of a dataset or distribution, scanning the URI against configured path prefixes and setting the appropriate step value.

### Ontological Validation

The system enforces **step lineage integrity** via database triggers:

- **No step downgrade**: A derived dataset cannot have a step value lower than its parent. For example, a PROCESSED dataset (step 2) cannot derive from a MAPPED dataset (step 3).
- **Distribution-Dataset alignment**: A distribution's step must always match the step of its parent dataset.
- **Derived distribution validation**: A derived distribution's step must be greater than or equal to the step of the source distribution it derives from.

---

## Automatic Versioning

Both Dataset and Distribution entities use automatic version incrementing. Every UPDATE operation triggers a function that increments the patch number of the semantic version (`major.minor.patch`). For example, updating a dataset at version `1.0.3` will automatically set its version to `1.0.4`.

This ensures a complete history of changes without requiring manual version management.

---

## Cross-Layer Derivation

Datasets support two types of derivation links:

- **`derived_card`** (vertical link): Every dataset must reference a `dataset_card`. This is the logical card that the dataset "belongs to." A Dataset Card may have datasets across multiple layers (RAW, PROCESSED, MAPPED) all referencing the same card.

- **`derived_dataset`** (horizontal/diagonal link): A dataset can optionally reference another dataset as its parent. This is used for **inter-layer derivation** -- for example, when a PROCESSED dataset is created from a RAW dataset. The ontological validation trigger ensures the derived dataset's step is always >= the parent's step.

Distributions follow a similar pattern with the `derived_from` field, which links a distribution to its source distribution. This can represent:

- **Intra-layer refinement** (same step): e.g., a filtered version of a distribution within the same layer.
- **Inter-layer processing** (different step): e.g., a MAPPED distribution derived from a PROCESSED distribution.
