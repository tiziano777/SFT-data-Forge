# 12. Admin Console

The Admin Console is a power-user tool for inspecting entity dependencies, analysing the impact of deletions before they happen, and keeping the database in sync with the physical file system.

> **Warning:** All deletion actions in this section are permanent. Each action writes a timestamped log file to `ui/admin_console/logs/` for audit purposes.

---

## Distribution Alignment Check

The alignment check identifies **ghost distributions**: entries in the database marked as `materialized=True` that have no corresponding file on disk.

### Running the Check

Click **🔍 Run alignment check**. The system iterates over all materialized distributions, maps each DB URI to its disk path, and reports any that are missing.

If orphans are found, each one is listed with its name and resolved disk path. Use the **✅ Select all** / **⬜ Deselect all** shortcuts or individual checkboxes to choose which orphans to act on.

### Available Actions

| Action | Description |
|---|---|
| **🗑️ Delete from DB** | Permanently deletes the selected distributions from the database |
| **📝 Set materialized=False** | Marks the distributions as not materialized, keeping the DB record but clearing the flag |

Both actions write an alignment log file to `ui/admin_console/logs/`.

---

## Entity Search

Use the search panel to locate the entity you want to inspect before viewing its dependency tree.

| Control | Options |
|---|---|
| **Entity type** | `dataset`, `distribution`, `dataset_card` |
| **Step filter** | `1`, `2`, `3`, or "All" (only for `dataset` and `distribution`) |
| **Language filter** | ISO language code (e.g. `it`, `en`) |
| **Text search** | Matches against name and description fields |

Results are shown as a dropdown. Select an entity to view its full dependency analysis below.

---

## Dependency View

### Dataset Card

Shows all attributes of the selected card and lists any datasets that reference it via `derived_card`. If dependent datasets exist, deletion is **blocked** and the blocking datasets are listed. If no datasets depend on the card, a **🗑️ Elimina Dataset Card** button becomes available.

### Dataset

Shows:
- **Parent card** (`derived_card` FK) — expander with full card attributes
- **Parent dataset** (`derived_dataset` FK) — expander with full dataset attributes
- **Child datasets** — datasets whose `derived_dataset` equals this dataset's ID
- **Child distributions** — distributions whose `dataset_id` equals this dataset's ID

**Deletion impact analysis:**

If any child distribution is referenced by a strategy (RESTRICT constraint), deletion is blocked and the blocking strategies are listed. Otherwise, the expected cascade effects are shown:

- Child distributions are deleted (CASCADE)
- Child datasets have `derived_dataset` set to NULL (SET NULL)

Click **🗑️ Elimina Dataset (con cascade)** to confirm. A log file is written listing all affected entities.

### Distribution

Shows:
- **Parent dataset** — expander with full dataset attributes
- **Parent distribution** (`derived_from` FK) — expander if applicable
- **Child distributions** — distributions whose `derived_from` equals this distribution's ID

**Deletion impact analysis:**

If strategies reference this distribution (RESTRICT), deletion is blocked. Otherwise:

- Child distributions have `derived_from` set to NULL (SET NULL)
- Associated mapping records are deleted (CASCADE)

Click **🗑️ Elimina Distribution** to confirm. A log file is written.

---

## Deletion Logs

Every deletion and alignment action writes a `.log` file to `ui/admin_console/logs/` with the format:

```
{entity_type}_{YYYYMMDD_HHMMSS}.log
alignment_{action}_{YYYYMMDD_HHMMSS}.log
```

Each log lists the entity type, UUID, and name of every affected entity. Logs can be viewed in the **Logs Management** section (section 11).
