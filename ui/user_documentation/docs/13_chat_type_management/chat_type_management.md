# 13. Chat Type Management

Management of the **Vocab Chat Type** vocabulary — the set of labels that identify which `apply_chat_template` strategy is applied to a distribution during model training preprocessing.

---

## What is a Chat Type

A Chat Type is a controlled-vocabulary entry that serves as a label in two places:

1. **Recipe strategies** — the `template_strategy` field of each strategy references a chat type code, telling the preprocessing pipeline which chat template to apply when building the training dataset.
2. **Schema binding** — each chat type is associated with exactly one **Schema Template** that defines the expected message structure and parameter contract for that strategy.

This binding ensures that every distribution processed under a given chat type uses a consistent, validated message format.

---

## Create New Chat Type

Click **📄 Create New Chat Type** to open the creation form.

| Field | Required | Description |
|---|---|---|
| **Code** | Yes | Unique short identifier used as the strategy label (e.g. `chatml`, `alpaca`) |
| **Description** | No | Human-readable explanation of the template strategy |
| **Schema** | Yes | Schema Template to bind to this chat type; selected from available schemas |

Click **💾 Save** to persist. The form closes and the new chat type becomes immediately available for use in recipe strategies and distribution processing.

---

## Browse & Search

Click **📑 Show Chat Types** to open the full list of chat types.

A search box filters results by code or description (case-insensitive). The match count is shown above the results.

Each chat type is displayed in a collapsible expander labelled `{code} — {description preview}`. Expanding it shows:

- **ID** — internal database UUID
- **Code**
- **Description**
- **Schema** — name and version of the associated Schema Template

---

## Edit a Chat Type

Click **✏️** inside a chat type expander to open an inline edit form. All three fields (code, description, schema) can be modified.

Click **💾 Save** to apply changes. The list refreshes automatically.

---

## Delete a Chat Type

Click **🗑️** inside a chat type expander to trigger a confirmation prompt. Click **Conferma** to permanently delete the entry or **Annulla** to abort.

> **Note:** Deleting a chat type that is referenced by existing recipe strategies may cause inconsistencies. Verify usage before deleting.

---

## Relationship to Other Modules

| Module | Dependency |
|---|---|
| **Data Studio** (section 7) | Chat type is selected per distribution during recipe creation |
| **Recipe Management** (section 10) | Chat type is editable per strategy in the Edit Strategies view |
| **Schema Templates** (section 9) | Each chat type must be bound to a Schema Template |
