CREATE EXTENSION IF NOT EXISTS pgcrypto;
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- DEFINIZIONE Vocabolari --

-- Tabella  source_category
CREATE TABLE vocab_source_category (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    code TEXT UNIQUE NOT NULL CHECK (code ~ '^[a-z_]+$'),
    description TEXT
);

-- Tabella Source_type
CREATE TABLE vocab_source_type (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    code TEXT UNIQUE NOT NULL CHECK (code ~ '^[a-z_]+$'),
    description TEXT
);

-- Tabella per field
CREATE TABLE vocab_field (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    code TEXT UNIQUE NOT NULL CHECK (code ~ '^[a-z_]+$'),
    description TEXT
);

-- Tabella per vertical
CREATE TABLE vocab_vertical (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    code TEXT UNIQUE NOT NULL CHECK (code ~ '^[a-z0-9_]+$'),
    description TEXT
);

-- tabella content
CREATE TABLE vocab_content (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    code TEXT UNIQUE NOT NULL CHECK (code ~ '^[a-z_]+$'),
    description TEXT
);

-- Tabella per core_skills
CREATE TABLE vocab_core_skill (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    code TEXT UNIQUE NOT NULL CHECK (code ~ '^[a-z_]+$'),
    description TEXT
);

-- Tabella per tasks
CREATE TABLE vocab_task (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    code TEXT UNIQUE NOT NULL CHECK (code ~ '^[a-z_]+$'),
    description TEXT
);

-- Tabella Skill-Task Taxonomy
CREATE TABLE skill_task_taxonomy (
    task_code TEXT NOT NULL REFERENCES vocab_task(code),
    skill_code TEXT NOT NULL REFERENCES vocab_core_skill(code),
    UNIQUE (task_code, skill_code)
);

-- Tabella per languages
CREATE TABLE vocab_language (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    code TEXT UNIQUE NOT NULL CHECK (code ~ '^[a-z_]+$'),
    description TEXT
);

CREATE TABLE schema_template (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    serial INTEGER GENERATED ALWAYS AS IDENTITY UNIQUE,
    name TEXT NOT NULL UNIQUE,
    description TEXT,
    "schema" JSONB NOT NULL,
    version TEXT NOT NULL DEFAULT '1.0' CHECK (version ~ '^\d+\.\d+(\.\d+)?$'),
    issued TIMESTAMPTZ DEFAULT NOW() NOT NULL,
    modified TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Tabella per chat types
CREATE TABLE vocab_chat_type (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    code TEXT UNIQUE NOT NULL CHECK (code ~ '^[a-z_0-9]+$'),
    description TEXT,
    schema_id UUID NOT NULL REFERENCES schema_template(id)
);

-- Tabella per dataset card types
CREATE TABLE vocab_dataset_type (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    code TEXT UNIQUE NOT NULL CHECK (code ~ '^[a-z_]+$'),
    description TEXT
);

-- Tabella per dataset split types
CREATE TABLE vocab_split (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    code TEXT UNIQUE NOT NULL CHECK (code ~ '^[a-z_]+$'),
    description TEXT
);

-- Tabella per licenses
CREATE TABLE vocab_license (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    code TEXT UNIQUE NOT NULL CHECK (code ~ '^[a-z0-9\-\.]+$'),
    description TEXT,
    license_url TEXT,
    note TEXT
);

CREATE TABLE vocab_modality (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    code TEXT UNIQUE NOT NULL CHECK (code ~ '^[a-z_]+$'),
    description TEXT,
    mime TEXT[]
);

