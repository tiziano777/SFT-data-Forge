# 6. Distribution

Granular management of physical data branches, structural analysis, and transformation between layers.

---

## What is a Distribution

If the Dataset is the logical container, the **Distribution** is the minimum operational unit. It represents a physical partition of the dataset (e.g., a specific split, a language, or a sampling phase) located within one of the three physical layers (**RAW**, **PROCESSED**, **MAPPED**).

SFT Data Forge allows you to operate on each Distribution independently, ensuring that every transformation is tracked and reproducible.

---

## Structure of a Distribution

Each Distribution follows a rigorous convention to ensure automation:

* **File Format**: Native support for standard formats (JSONL, Parquet, and their compressed extensions).
* **Naming Convention**: Folder names reflect the metadata (e.g., `lang=it`, `split=train`).
* **Physical Path**: Dynamic placement based on the layer of ownership, but consistent with the metadata of its parent Dataset Card.

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
| `step`            | Layer indicator: 1=RAW, 2=PROCESSED, 3=MAPPED. Auto-detected.   | Yes (auto)   |

---

## Visualization and Inspection

From the dashboard, once you have selected the layer workflow (RAW / PROCESSED / MAPPED Dataset Distribution Workflow), you can explore the distributions associated with the selected dataset. This procedure is guided -- it is not possible to select unregistered datasets.

---

## Workflow: Src Schema, Mapping, and Load

This is the transformation lifecycle of a Distribution toward the next layer (or its finalization for a model recipe):

1. **Src Schema Extraction**: SFT Data Forge analyzes the distribution to extract the source schema (using the `genson` package). It identifies keys, data types, and nested structures.
2. **Mapping Definition**: The user defines how the source fields must be transformed or renamed to adhere to the destination standard. Cleaning logic can be applied during this phase.
3. **Load (To Next Layer)**: Physical execution of the transformation. Data is read from the current distribution, processed according to the mapping, and written to the next layer (e.g., from RAW to PROCESSED).

---

## Dashboard SQL

SFT Data Forge integrates a powerful engine based on **DuckDB** for querying loaded distributions in real time.

### Basic Queries

Standard SQL queries can be executed directly on the distribution files to validate data:

```sql
SELECT * FROM {distribution_table} WHERE _lang = 'it' LIMIT 10;
```

This mode is available at every physical layer (RAW, PROCESSED, MAPPED).

### Join Queries with Statistics

The system allows advanced joins between distribution data and the counts table saved in the STATS silo:

```sql
-- Example join to filter records based on pre-computed statistics
SELECT d.*, s.token_count
FROM {distribution_table} d
JOIN 'path/to/STATS_DIRECTORY/low_level_stats/*.parquet' s ON d._id_hash = s._id_hash
WHERE s.token_count > 50; -- arbitrary condition
```

### Statistics and Counts

For each distribution, SFT Data Forge allows generating detailed analytical reports:

**Available Metrics:**
- Total record count.
- Length distributions (characters/words).
- Token statistics (if the tokenizer is configured).
- Metadata density analysis (how many records have null fields).

**Computation:** Counts are executed via optimized pipelines and results are persisted in Parquet format in the dedicated statistics folder, ready to be used in Dashboard SQL or for quality reports.

There are two types of statistics:
* `low_level_stats`: Statistics and counts computed for each sample of the distribution. Available only for PROCESSED and MAPPED layers.
* `chat_template_stats`: Statistics based on the current enterprise schema template. Available only for MAPPED distributions.

---

## Mapper (PROCESSED Layer)

In the PROCESSED layer, a module is available that allows mapping any dataset from a `src_schema` to a `dst_schema` using a complete mapping language and a loader that applies the rules to the data. This method, in addition to being flexible, does not require custom scripts, and every mapped sample passes a validation check -- avoiding errors that a normal script might silently ignore.

### Arbitrary Transformations and Filters

Beyond standard mapping, for greater flexibility, UDF (User Defined Functions) transformation scripts can be injected at the distribution level by defining Python functions.

This allows you to "distill" the content (e.g., PII/tag removal, data extraction, concatenations, keyword filtering, text normalization) before the data is consolidated into the next layer, ensuring total control over content quality.

Each function is metadated, tested, and saved to the DB, ensuring reusability and tracking.

### Mapping Logic

For each key of the selected destination schema, a rule must be defined. Here is an example:

```json
{
    "dst_schema_key_1": ["function_name", "arg1", "arg2"],
    "dst_schema_key_2": ["src_schema_key"]
}
```

In the simplest case (direct mapping), the array has a single value referencing a key from the `src_schema` (1:1 mapping).

For complex transformations, for each key you specify an array of values. The first element is always either the name of a function to apply on one or more arguments. Each argument can be a reference to a `src_schema` key or a value disconnected from it (string, int, list...).

### Core Available Functions

The mapper has built-in functions that cover simple use cases. For specific functionality, you can define your own functions using UDFs.

> **WARNING:** Every function, even `set_fixed_value` which returns a single value, must return a list containing the output.

#### Available Core Functions:

| Function                | Description                                             | Syntax                                                          |
|------------------------|---------------------------------------------------------|-----------------------------------------------------------------|
| `set_fixed_value`      | Set to a fixed value                                    | `['set_fixed_value', <fixed_str or src_path or null>]`          |
| `concat`               | Concatenates multiple string values.                    | `['concat', <fixed_str1 or src_path1>, ...]`                    |
| `map_enum`             | Maps a source value to a destination value via dict.    | `['map_enum', 'src_path', {'src_val1': 'dst_val1', ...}]`      |
| `remove_strings`       | Remove fixed strings                                    | `['remove_strings', 'tag_name', 'src_path']`                    |
| `remove_regex_strings` | Remove strings matching regex patterns                  | `['remove_regex_strings', 'src_path', ['regex1', 'regex2']]`   |
| `remove_prefix`        | Removes a prefix from a string field                    | `['remove_prefix', 'prefix_to_remove', 'src_path']`            |
| `extract_tag_content`  | Extracts tag content from first regex match             | `['extract_tag_content', 'tag_name', 'src_path']`              |
| `remove_tag_content`   | Remove tags and their contents                          | `['remove_tag_content', 'tag_name', 'src_path']`               |

> **NOTE:** Beyond the core functions, the AI mapping inference engine recognizes additional functions (e.g., `serialize_to_json_list`, `extract_and_aggregate_tag_object_content_id`, `insert_lang_system_prompt`) that can be implemented as UDFs and dynamically loaded from the database.

### UDF Creation Rules

In the mapping section, the user can write their own function to use for mapping samples to the selected destination schema:

```python
def user_defined_function(func_name: str, **kwargs) -> Union[list[str], str]:
    """
    func_name: always required as the first parameter (str)
    **kwargs: additional optional parameters named `param_[i]` (int, float, list, dict)
    It is best to define them one by one if static,
    including the input type you expect!
    """
    # Implement your logic here #
    # You can return a list of strings OR a single string
    # Mapper accepts only list and str as output
    return ["result1", "result2"]
```

The function goes through a testing phase where the user passes example parameters for debugging. Once the function is approved and validated, the new UDF is saved to the DB.

### Mapping Workflow

1. **Select Destination Schema** -- Choose from the `schema_template` entries saved in the DB.
2. **Define UDFs (optional)** -- Write custom functions if the core ones are insufficient.
3. **Write Mapping Rules in JSON** -- For each destination key, define the transformation operation.
4. **Run Mapping on a Sample** -- Execute the mapping on a test sample for debugging and validation.
5. **Store Mapping in DB** -- Save the mapping with automatic versioning.
