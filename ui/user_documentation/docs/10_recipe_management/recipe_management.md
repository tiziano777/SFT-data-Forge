# 10. Recipe Management

Centralized view for browsing, editing, importing, and deleting existing recipes. This section is complementary to the Data Studio (section 7), which covers the interactive creation of new recipes from scratch.

---

## Overview

Recipe Management provides a full CRUD interface over the recipes stored in the Metadata DB. Each recipe is displayed as a collapsible card that aggregates its strategies, statistics, and actions in one place.

---

## Import Recipe from YML

An existing recipe definition can be imported via a `.yml` / `.yaml` file using the **Import Recipe from YML** expander at the top of the page.

### Required YAML Structure

```yaml
name: "my-recipe"
description: "Short description"
scope: "sft"
tasks:
  - "chat"
entries:
  <distribution_uri>:
    dist_id: "<uuid>"
    chat_type: "<chat_type_code>"
    replica: 2
    system_prompt:
      - "You are a helpful assistant."
    system_prompt_name:
      - "assistant_prompt"
```

**Required top-level fields:** `name`, `description`, `scope`, `tasks`, `entries`

**Required per entry:** `dist_id`, `chat_type`, `replica`

The `system_prompt` and `system_prompt_name` lists must have the same length. Each position is a name–content pair: the system prompt is created in the DB if it does not already exist, and then linked to the strategy.

### Import Logic

| Scenario | Behaviour |
|---|---|
| `id` in YAML matches existing recipe UUID | New recipe created with a new UUID; `derived_from` set to the original UUID |
| Name exists, no `id` provided | Existing recipe is updated (upsert) |
| New name, no `id` provided | New recipe inserted |

---

## Search & Filter

- **Search**: Free-text search across recipe name and description fields.
- **Language filter**: Dropdown to restrict results by language (populated from available distributions).
- A counter shows how many recipes match the active filters.

---

## Recipe Card

Each recipe is displayed in a collapsible expander. The header shows:

- **Name** and **version**
- Number of strategies
- Total **samples** and **tokens** (sum across all strategies, weighted by replica factor)

Expanding the card reveals:

| Section | Description |
|---|---|
| **Metadata** | Description, scope, version, issued date, modified date |
| **Tags** | Highlighted tag badges |
| **Tasks** | Task badges (e.g., `chat`, `summarization`) |
| **Totals** | Samples, tokens, words metrics |
| **Language distribution** | Pie chart showing sample share per language |
| **Strategies table** | Distribution name, language, replica factor (`×N`), chat type, samples, assigned system prompts |

---

## Edit Metadata

Click the **📝** button to open the metadata editor form for a recipe.

Editable fields:
- **Recipe Name** (required)
- **Scope** (required — vocabulary-controlled)
- **Description**
- **Tasks** (multiselect — vocabulary-controlled)
- **Tags** (comma-separated)
- **Derived From** (optional — link to a parent recipe by name)

Changes are persisted immediately on **Save**. The `modified` timestamp is updated automatically.

---

## Edit Strategies

Click the **✏️** button to enter strategy edit mode for a recipe.

The inline table allows modifying each strategy row:

| Column | Description |
|---|---|
| **Distribution** | Read-only: name and language of the linked distribution |
| **Replica** | Integer factor — how many times this distribution is sampled |
| **Chat Type** | Selectbox of available chat type codes |
| **System Prompts** | Multiselect of available system prompt names |
| **Delete** | Checkbox to mark a strategy for deletion |

Click **Save Changes** to apply all edits atomically. Strategies marked for deletion are removed (cascade removes associated `strategy_system_prompt` rows). Click **Cancel** to discard all pending changes.

---

## Download YML

The **📥 Download YML** button is always visible in the recipe card action area. It exports the full recipe structure — including all strategies, distribution IDs/URIs, replica factors, chat types, and system prompt names — as a YAML file named `{recipe_name}_recipe.yaml`.

This file can be re-imported on another environment using the Import feature described above.

---

## Delete Recipe

Click the **🗑️** button to start a two-step deletion:

1. The button label changes to a confirmation prompt showing the recipe name.
2. Confirm with **✅ Sì** or abort with **❌**.

Deletion is permanent and removes the recipe along with all its strategies and `strategy_system_prompt` associations (via DB cascade).
