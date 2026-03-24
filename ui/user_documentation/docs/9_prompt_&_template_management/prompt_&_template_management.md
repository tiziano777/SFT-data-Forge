# 9. Prompt & Template Management

Centralized management of system prompts and enterprise schema templates to ensure coherence and standardization during model instruction phases.

---

## System Prompt Management

### What is a System Prompt

The **System Prompt** is the set of high-level instructions that defines the behavior, tone, and constraints of the language model. In SFT Data Forge, prompts are treated as versioned and traceable entities, fundamental to the flexibility of training recipes.

### Creation and Editing

Creation is done through a dedicated form that requires:

* **Core Metadata**: Unique name, functional description, and language (ISO code).
* **Technical Parameters**: Prompt text, Quality Score (0.0 - 1.0), and length (automatically calculated if not specified).
* **Inheritance**: Ability to indicate whether the prompt derives from an existing one (**Derived From**), creating a graph of logical parentage.
* **Editing**: Modification allows updating every field, automatically registering the `modified` timestamp.

### Search and Filters

To manage large volumes of instructions, the system offers advanced search tools:

* **Full-text Search**: Search by keywords within the name, description, or body of the prompt.
* **Language Filter**: Multi-language selection via dedicated toggle.
* **Length Range Filter**: A dynamic slider (based on the maximum length present in the DB) allows filtering prompts by character count.

### Versioning and Preview

Each prompt carries its own version (e.g., `1.0`). The version is **automatically incremented** on every update (patch number is bumped). Through the **Expander** interface, you can preview the prompt code and its technical metadata (issue date, quality score, version) without leaving the main list.

> **NOTE:** When the prompt text is updated, the previous version is automatically saved to the `system_prompt_history` table via a database trigger, ensuring a complete change history.

### Deletion

Deletion is protected by a **Confirm Delete** mechanism. The user must confirm the operation within the expander.

> **IMPORTANT:** Deletion is a **soft delete**: the prompt is not physically removed from the database but is marked as `deleted = TRUE`. This preserves provenance integrity -- deleted prompts remain visible in lineage graphs (shown in red) but cannot be used in new recipes. A prompt that is currently referenced by an active recipe strategy cannot be deleted (enforced via FK constraint).

---

## Schema Template & Chat Type Management

### What is a Schema Template

The **Schema Template** defines the target data structure for distributions. It is the "target" of the Mapper module: without a defined schema template, the system cannot guarantee the standardization necessary for loading into the MAPPED layer.

> **NOTE:** When the schema JSON is updated, the previous version is automatically saved to the `schema_template_history` table via a database trigger. Version auto-increment also applies to schema templates.

### Creating a New Chat Type

The **Chat Type** (Vocab Chat Type) is the entity that links a technical schema to a conversation strategy.

* **Code**: Short identifier for the chat strategy.
* **Schema Mapping**: Every Chat Type must be associated with an existing **Schema Template**, selectable via guided search. This is enforced by a foreign key constraint (`schema_id`) -- it is not possible to create a Chat Type without linking it to an existing schema.
* **Description**: Explanation of the use case (e.g., "Multi-turn assistant", "Chain of Thought").

### Search and Template Management

SFT Data Forge allows managing the chat type vocabulary dynamically:

* **Listing & Search**: Display of all registered chat types with clear indication of the linked schema ID.
* **Safe Management**: Editing and deletion follow the same safe flow as System Prompts, ensuring that entities currently used in active recipes are not removed without notice.
