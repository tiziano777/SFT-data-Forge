-- Tabella per lo storico degli system_prompt
CREATE TABLE system_prompt_history (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    system_prompt_id UUID NOT NULL,
    name TEXT NOT NULL UNIQUE,
    description TEXT NOT NULL,
    prompt TEXT NOT NULL,
    _lang TEXT NOT NULL,
    length INTEGER NOT NULL,
    version TEXT NOT NULL DEFAULT '1.0' CHECK (version ~ '^\d+\.\d+(\.\d+)?$'),
    issued TIMESTAMPTZ DEFAULT NOW() NOT NULL,
    modified TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT fk_system_prompt_language
        FOREIGN KEY (_lang)
        REFERENCES vocab_language(code)
        ON UPDATE CASCADE,
    CONSTRAINT fk_system_prompt_history_parent
        FOREIGN KEY (system_prompt_id)
        REFERENCES system_prompt(id)
        ON DELETE CASCADE
);

-- Tabella per lo storico degli schema_template
CREATE TABLE schema_template_history (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    serial INTEGER,
    schema_template_id UUID NOT NULL,
    schema JSONB NOT NULL,
    version TEXT NOT NULL,
    modified_at TIMESTAMPTZ DEFAULT NOW() NOT NULL,
    CONSTRAINT fk_schema_template_history_parent
        FOREIGN KEY (schema_template_id)
        REFERENCES schema_template(id)
        ON DELETE CASCADE
);

-- Tabella per lo storico dei mapping principali
CREATE TABLE mapping_history (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    mapping_id UUID NOT NULL,
    schema_template_id UUID NOT NULL,
    mapping JSONB NOT NULL,
    version TEXT NOT NULL,
    modified_at TIMESTAMPTZ DEFAULT NOW() NOT NULL,
    CONSTRAINT fk_mapping_history_parent
        FOREIGN KEY (mapping_id)
        REFERENCES mapping(id)
        ON DELETE CASCADE
);


-- Tabella History per le Strategy
CREATE TABLE history_strategy (
    history_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    original_strategy_id UUID NOT NULL,
    recipe_id UUID NOT NULL,
    distribution_id UUID NOT NULL,
    replication_factor INTEGER,
    template_strategy TEXT,
    operation_type TEXT NOT NULL, -- 'UPDATE' o 'DELETE'
    modified_at TIMESTAMPTZ DEFAULT NOW() NOT NULL
);
-- Tabella History per la relazione N:N con i System Prompt
CREATE TABLE history_strategy_system_prompt (
    history_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    original_strategy_id UUID NOT NULL,
    system_prompt_name TEXT NOT NULL,
    operation_type TEXT NOT NULL,
    modified_at TIMESTAMPTZ DEFAULT NOW() NOT NULL
);