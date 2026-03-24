# 3. Dataset Card Management

Management of descriptive cards registered in the SFT Data Forge system.

---

## What is a Dataset Card

The **Dataset Card** represents the top-level logical layer of the system. It acts as a universal descriptor (metadata) to track datasets within the ecosystem, regardless of their physical state.

The system distinguishes between two types of entities:

* **Materialized**: Datasets whose data has already been acquired and physically resides in one or more layers of the filesystem (RAW, PROCESSED, MAPPED).
* **Non-Materialized**: Datasets awaiting download or candidates for acquisition (watchlist) that you want to monitor before actual import.

---

## Inserting a New Card

The insertion of a new entity is done through a **guided procedure (Wizard)**. This tool walks the user step-by-step through filling in the required information, ensuring that every dataset is properly registered before entering the processing pipelines.

The Wizard validates input data and populates the **Metadata DB**, creating the logical pointer that will later enable materialization on disk.

---

## Exploration and Editing

Users can manage existing cards through an advanced monitoring interface:

* **Search Bar & Filters**: Navigate the card catalog by name and other criteria.
* **Status Visualization**: Immediately identify whether the dataset is only a descriptor (non-materialized) or also has a physical binding on disk.
* **Editing**: Ability to update descriptive info and metadata even after initial creation.
* **Deletion**: Ability to delete a dataset descriptor.

> **WARNING:** Deleting a card will also remove its binding to any physical datasets. Additionally, a card **cannot be deleted** if any Dataset entity in the system references it via `derived_card` -- the system enforces this constraint via a protection trigger.

---

## Download / Materialization (HuggingFace)

For non-materialized datasets that indicate **HuggingFace (HF)** as their source, the Dataset Card enables direct operational workflows:

1. **Trigger**: The download procedure can be initiated directly from the card.
2. **Process**: The system acquires data from HF and physically allocates it in the **RAW Layer** of the filesystem.
3. **Update**: Upon completion, the Card changes status to "Materialized" and makes the transformation and mapping modules available.

> **IMPORTANT:** The automated download procedure via the dashboard is currently limited to HuggingFace sources. For other sources, manual scripting modules are used. A dedicated dataset acquisition section has been provided for registering data from different download sources.
> For control purposes, the procedure is split: SFT Data Forge returns the CLI command (prototype) to paste and execute on the host machine for download, after which you must return to the download procedure window to continue acquiring metadata for the newly downloaded dataset.

---

## Card Fields and Metadata

Each Dataset Card entity is described by the following fields:

| Field                | Description                                                                 | Required     |
|----------------------|-----------------------------------------------------------------------------|--------------|
| `dataset_name`       | Name of the dataset (unique).                                               | Yes          |
| `dataset_id`         | Unique identifier of the dataset (unique).                                  | Yes          |
| `modality`           | Modality of the dataset (e.g., text, image). From `vocab_modality`.         | Yes          |
| `dataset_description`| Description of the dataset.                                                 | No           |
| `publisher`          | Name of the dataset publisher.                                              | No           |
| `notes`              | Additional notes about the dataset.                                         | No           |
| `source_url`         | Source URL of the dataset. Must be a valid URL if provided.                 | No           |
| `download_url`       | Download URL for the dataset. Must be a valid URL if provided.              | No           |
| `languages`          | Languages of the dataset. Must contain at least one element.                | Yes          |
| `license`            | License of the dataset. Default: `unknown`. From `vocab_license`.           | Yes          |
| `core_skills`        | Primary skills associated with the dataset. From `vocab_core_skill`.        | No           |
| `tasks`              | Tasks associated with the dataset. From `vocab_task`.                       | No           |
| `sources`            | Sources of the dataset.                                                     | No           |
| `source_type`        | Source type of the dataset. From `vocab_source_type`.                       | No           |
| `fields`             | Domain fields of the dataset.                                               | No           |
| `vertical`           | Vertical sectors associated with the dataset.                               | No           |
| `contents`           | Content types of the dataset.                                               | No           |
| `has_reasoning`      | Whether the dataset includes reasoning. Default: `FALSE`.                   | No           |
| `quality`            | Quality of the dataset (1-5). Default: `1`.                                 | No           |
| `last_update`        | Timestamp of the last update (automatic).                                   | Yes (auto)   |
| `created_at`         | Timestamp of card creation (automatic).                                     | Yes (auto)   |

---

## Card Composition (Parent/Child)

Dataset Cards support hierarchical composition through parent/child relationships. A parent card can aggregate multiple child cards to represent complex datasets that are composed of smaller, independently managed sub-datasets.

The relationship is stored in the `card_composition` table with the following properties:

* **Weight**: An optional field (0.00 to 1.00) representing the proportional weight of the child card within the composition.
* **No self-reference**: A card cannot reference itself as a child.
* **Cascade behavior**: If a parent or child card is renamed, the composition automatically updates. If a card is deleted, the composition rows referencing it are removed.

---

## Skill/Task Taxonomy Coherence

When defining `core_skills` and `tasks` for a Dataset Card, the system enforces **taxonomic coherence** via a validation trigger:

- If `core_skills` are specified, they must be logically compatible with the selected `tasks` according to the `skill_task_taxonomy` table.
- Each declared skill must map to at least one of the selected tasks.
- The special value `mix` can be used for either skills or tasks to bypass the coherence check (useful for multi-domain datasets).
- If skills are declared without any tasks, the system raises an error -- skills must always be anchored to practical tasks.
