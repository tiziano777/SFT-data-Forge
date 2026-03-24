---*** DEFINIZIONE TABELLE PRINCIPALI ***---
-- Tabella per la configurazione dei percorsi
CREATE TABLE config_paths (
    layer_name TEXT PRIMARY KEY,
    path_prefix TEXT NOT NULL,
    step_value SMALLINT NOT NULL
);

CREATE TABLE dataset_card (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    dataset_name TEXT NOT NULL,
    dataset_id TEXT NOT NULL UNIQUE, 
    modality TEXT NOT NULL DEFAULT 'text',
    dataset_description TEXT,
    publisher TEXT,
    notes TEXT,
    source_url TEXT CHECK (source_url IS NULL OR source_url='' OR source_url ~ '^https?://'),
    download_url TEXT CHECK (download_url IS NULL OR download_url='' OR download_url ~ '^https?://'),
    languages TEXT[] NOT NULL CHECK (array_length(languages, 1) >= 1),
    license TEXT NOT NULL DEFAULT 'unknown',
    core_skills TEXT[] DEFAULT '{}'::TEXT[] CHECK (array_length(core_skills, 1) <= 1),
    tasks TEXT[] DEFAULT '{}'::TEXT[] CHECK (array_length(tasks, 1) <= 1),
    sources TEXT[] DEFAULT '{}'::TEXT[],
    source_type TEXT,
    fields TEXT[] DEFAULT '{}'::TEXT[],
    vertical TEXT[] DEFAULT '{}'::TEXT[],
    contents TEXT[] DEFAULT '{}'::TEXT[],
    has_reasoning BOOLEAN DEFAULT FALSE,
    last_update TIMESTAMPTZ DEFAULT NOW() NOT NULL,
    created_at TIMESTAMPTZ DEFAULT NOW() NOT NULL,
    quality SMALLINT DEFAULT 1 CHECK (quality >= 1 AND quality <= 5),
    CONSTRAINT uq_dataset_card_name UNIQUE (dataset_name),
    CONSTRAINT fk_dataset_card_license
        FOREIGN KEY (license)
        REFERENCES vocab_license(code)
        ON UPDATE CASCADE,
    CONSTRAINT fk_dataset_card_modality
        FOREIGN KEY (modality)
        REFERENCES vocab_modality(code)
        ON UPDATE CASCADE,
    CONSTRAINT fk_dataset_card_source_type
        FOREIGN KEY (source_type)
        REFERENCES vocab_source_type(code)
        ON UPDATE CASCADE
);

CREATE TABLE card_composition (
    parent_card_name TEXT,
    child_card_name TEXT,
    weight NUMERIC(3,2) DEFAULT NULL CHECK (weight >= 0 AND weight <= 1), -- Quantità tra 0.00 e 1.00
    PRIMARY KEY (parent_card_name, child_card_name),

    CONSTRAINT fk_parent_card
        FOREIGN KEY (parent_card_name) REFERENCES dataset_card(dataset_name)
        ON UPDATE CASCADE ON DELETE CASCADE,
    CONSTRAINT fk_child_card
        FOREIGN KEY (child_card_name) REFERENCES dataset_card(dataset_name)
        ON UPDATE CASCADE ON DELETE CASCADE,
    CONSTRAINT no_self_reference_card CHECK (parent_card_name <> child_card_name)
);

CREATE TABLE dataset (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    uri TEXT UNIQUE NOT NULL CHECK (uri ~ '^[a-zA-Z][a-zA-Z0-9+.-]*://'),
    derived_card UUID NOT NULL,
    derived_dataset UUID,
    dataset_type TEXT,
    globs TEXT[] NOT NULL DEFAULT '{}',
    languages TEXT[] NOT NULL CHECK (array_length(languages, 1) >= 1),
    name TEXT UNIQUE NOT NULL,
    description TEXT,
    source TEXT,
    version TEXT NOT NULL DEFAULT '1.0' CHECK (version ~ '^\d+\.\d+(\.\d+)?$'),
    issued TIMESTAMPTZ DEFAULT NOW() NOT NULL,
    modified TIMESTAMPTZ DEFAULT NOW() NOT NULL,
    license TEXT NOT NULL DEFAULT 'unknown',
    step SMALLINT DEFAULT 1 CHECK (step >= 1 AND step <= 3),
    CONSTRAINT fk_dataset_license
        FOREIGN KEY (license)
        REFERENCES vocab_license(code)
        ON UPDATE CASCADE,
    CONSTRAINT fk_derived_card_dataset
        FOREIGN KEY (derived_card)
        REFERENCES dataset_card(id)
        ON DELETE SET NULL
        ON UPDATE CASCADE,
    CONSTRAINT fk_derived_dataset
        FOREIGN KEY (derived_dataset)
        REFERENCES dataset(id)
        ON DELETE SET NULL
        ON UPDATE CASCADE,
    CONSTRAINT fk_vocab_dataset_type
        FOREIGN KEY (dataset_type)
        REFERENCES vocab_dataset_type(code)
        ON UPDATE CASCADE
);

CREATE TABLE system_prompt (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name TEXT NOT NULL UNIQUE,
    description TEXT NOT NULL,
    prompt TEXT NOT NULL,
    _lang TEXT NOT NULL DEFAULT 'un', -- ISO-2 LANG
    length INTEGER NOT NULL,
    derived_from UUID REFERENCES system_prompt(id),
    quality_score DECIMAL(3, 2) DEFAULT 0.0,
    deleted BOOLEAN DEFAULT FALSE, 
    version TEXT NOT NULL DEFAULT '1.0' CHECK (version ~ '^\d+\.\d+(\.\d+)?$'),
    issued TIMESTAMPTZ DEFAULT NOW() NOT NULL,
    modified TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT fk_system_prompt_language
        FOREIGN KEY (_lang)
        REFERENCES vocab_language(code)
        ON UPDATE CASCADE
);

CREATE TABLE distribution (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    uri TEXT NOT NULL CHECK (uri ~ '^[a-zA-Z][a-zA-Z0-9+.-]*://'),
    tokenized_uri TEXT,
    dataset_id UUID NOT NULL,
    name TEXT NOT NULL UNIQUE,
    glob TEXT NOT NULL,
    format TEXT NOT NULL,
    query TEXT DEFAULT NULL,
    script TEXT DEFAULT NULL,
    derived_from UUID,
    src_schema JSONB DEFAULT '{}'::jsonb NOT NULL,
    description TEXT,
    lang TEXT NOT NULL DEFAULT 'un',
    split TEXT,
    materialized BOOLEAN NOT NULL DEFAULT TRUE,
    tags TEXT[] DEFAULT '{}'::TEXT[],
    license TEXT NOT NULL DEFAULT 'unknown',
    version TEXT NOT NULL DEFAULT '1.0' CHECK (version ~ '^\d+\.\d+(\.\d+)?$'),
    issued TIMESTAMPTZ DEFAULT NOW() NOT NULL,
    modified TIMESTAMPTZ DEFAULT NOW() NOT NULL,
    step SMALLINT DEFAULT 1 CHECK (step >= 1 AND step <= 3),
    
    CONSTRAINT uq_distribution_uri UNIQUE (uri),
    CONSTRAINT fk_distribution_dataset
        FOREIGN KEY (dataset_id)
        REFERENCES dataset(id)
        ON DELETE CASCADE
        ON UPDATE CASCADE,
    CONSTRAINT fk_distribution_license
        FOREIGN KEY (license)
        REFERENCES vocab_license(code)
        ON UPDATE CASCADE,
    CONSTRAINT fk_vocab_distribution_split
        FOREIGN KEY (split)
        REFERENCES vocab_split(code)
        ON UPDATE CASCADE,
    CONSTRAINT fk_distribution_derived
        FOREIGN KEY (derived_from)
        REFERENCES distribution(id)
        ON DELETE SET NULL
        ON UPDATE CASCADE
);

CREATE TABLE mapping (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    serial INTEGER GENERATED ALWAYS AS IDENTITY UNIQUE,

    distribution_id UUID NOT NULL, -- SRC DISTRIBUTION TO MAP FROM
    schema_template_id UUID NOT NULL, -- TARGET SCHEMA TEMPLATE TO MAP TO
    mapping JSONB DEFAULT '{}'::jsonb NOT NULL, -- MAPPING RULES FOR CONTENT
    
    version TEXT NOT NULL DEFAULT '1.0' CHECK (version ~ '^\d+\.\d+(\.\d+)?$'),
    issued TIMESTAMPTZ DEFAULT NOW() NOT NULL,
    modified TIMESTAMPTZ DEFAULT NOW() NOT NULL,
    UNIQUE (schema_template_id, distribution_id),
    CONSTRAINT fk_mapping_distribution
        FOREIGN KEY (distribution_id)
        REFERENCES distribution(id)
        ON DELETE CASCADE,
    CONSTRAINT fk_mapping_schema_template
        FOREIGN KEY (schema_template_id)
        REFERENCES schema_template(id)
        ON DELETE CASCADE
);

CREATE TABLE UDF(
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name TEXT NOT NULL UNIQUE,
    description TEXT,
    function_definition TEXT NOT NULL,
    example_params TEXT[] ,
    issued TIMESTAMPTZ DEFAULT NOW() NOT NULL,
    modified TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE recipe (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name TEXT NOT NULL UNIQUE,
    description TEXT NOT NULL,
    scope TEXT NOT NULL,
    tasks TEXT[] DEFAULT '{}'::TEXT[],
    tags TEXT[] DEFAULT '{}'::TEXT[],
    issued TIMESTAMPTZ DEFAULT NOW() NOT NULL,
    modified TIMESTAMPTZ DEFAULT NOW() NOT NULL,
    version TEXT DEFAULT '1.0.0' CHECK (version ~ '^\d+\.\d+\.\d+$'),

    derived_from UUID,
    CONSTRAINT fk_recipe_derived_from
        FOREIGN KEY (derived_from)
        REFERENCES recipe(id)
        ON DELETE SET NULL
        ON UPDATE CASCADE,

    CONSTRAINT fk_recipe_scope
        FOREIGN KEY (scope)
        REFERENCES vocab_dataset_type(code)
        ON UPDATE CASCADE
);

CREATE TABLE strategy (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    recipe_id UUID NOT NULL,
    distribution_id UUID NOT NULL,

    replication_factor INTEGER DEFAULT 1 CHECK (replication_factor >= 0),
    template_strategy TEXT REFERENCES vocab_chat_type(code) ON UPDATE CASCADE,

    issued TIMESTAMPTZ DEFAULT NOW() NOT NULL,
    modified TIMESTAMPTZ DEFAULT NOW() NOT NULL,

    CONSTRAINT uq_recipe_distribution UNIQUE (recipe_id, distribution_id),
    
    CONSTRAINT fk_strategy_recipe
        FOREIGN KEY (recipe_id)
        REFERENCES recipe(id)
        ON DELETE CASCADE
        ON UPDATE CASCADE,
        
    CONSTRAINT fk_strategy_distribution
        FOREIGN KEY (distribution_id)
        REFERENCES distribution(id)
        ON DELETE RESTRICT
        ON UPDATE CASCADE
);

-- 2.Relazione N:N tra Strategy e System Prompt
CREATE TABLE strategy_system_prompt (
    strategy_id UUID NOT NULL,
    system_prompt_name TEXT NOT NULL,
    
    PRIMARY KEY (strategy_id, system_prompt_name),

    CONSTRAINT fk_ssp_strategy
        FOREIGN KEY (strategy_id)
        REFERENCES strategy(id)
        ON DELETE CASCADE
        ON UPDATE CASCADE,
        
    CONSTRAINT fk_ssp_prompt
        FOREIGN KEY (system_prompt_name)
        REFERENCES system_prompt(name)
        ON DELETE RESTRICT -- Impedisce di eliminare un prompt se usato in una ricetta
        ON UPDATE CASCADE
);

CREATE TABLE checkpoint (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    recipe_id UUID NOT NULL,
    -- model_name TEXT NOT NULL REFERENCES model(name) ON DELETE SET NULL ON UPDATE CASCADE,
    checkpoint_number INTEGER NOT NULL CHECK (checkpoint_number >= 1),
    src_uri TEXT NOT NULL CHECK (src_uri ~ '^[a-zA-Z][a-zA-Z0-9+.-]*://'),
      
    name TEXT NOT NULL,
    description TEXT,
    
    results JSONB DEFAULT '{}'::JSONB, 
    hyperparams JSONB DEFAULT '{}'::JSONB,

    issued TIMESTAMPTZ DEFAULT NOW() NOT NULL,

    CONSTRAINT uq_recipe_checkpoint_number UNIQUE (recipe_id, checkpoint_number),
    
    CONSTRAINT fk_checkpoint_recipe
        FOREIGN KEY (recipe_id)
        REFERENCES recipe(id)
        ON DELETE CASCADE
        ON UPDATE CASCADE
);

-- CREATE TABLE model(id,name, description, tokenizer, architecture, version, issued, modified);

