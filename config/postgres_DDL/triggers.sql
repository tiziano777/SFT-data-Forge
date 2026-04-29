-- TRIGGER FUNCTIONS --

-- Trigger function per l'assegnazione dei valori di step
CREATE OR REPLACE FUNCTION fn_update_step_from_uri()
RETURNS TRIGGER AS $$
DECLARE
    detected_step SMALLINT;
BEGIN
    -- Cerchiamo se l'URI contiene uno dei path configurati
    -- Usiamo LIKE con % per verificare se il path è contenuto nell'URI
    SELECT step_value INTO detected_step
    FROM config_paths
    WHERE NEW.uri LIKE '%' || path_prefix || '%'
    LIMIT 1;

    -- Se troviamo una corrispondenza, aggiorniamo lo step
    IF detected_step IS NOT NULL THEN
        NEW.step := detected_step;
    END IF;

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Trigger per la tabella dataset
DROP TRIGGER IF EXISTS trg_set_step_dataset ON dataset;
CREATE TRIGGER trg_set_step_dataset 
    BEFORE INSERT OR UPDATE ON dataset 
    FOR EACH ROW EXECUTE FUNCTION fn_update_step_from_uri();

-- Trigger per la tabella distribution
CREATE TRIGGER trg_set_step_distribution
    BEFORE INSERT OR UPDATE ON distribution 
    FOR EACH ROW
    EXECUTE FUNCTION fn_update_step_from_uri();

-- Trigger Function Funzione di Normalizzazione Vocabolari
CREATE OR REPLACE FUNCTION normalize_vocab_code()
RETURNS TRIGGER AS $$
BEGIN
    -- Normalizza il code: minuscolo, sostituisce spazi con underscore
    -- MA mantiene numeri e altri caratteri validi
    NEW.code := LOWER(TRIM(NEW.code));
    NEW.code := REGEXP_REPLACE(NEW.code, '[-\s]+', '_', 'g');
    NEW.code := REGEXP_REPLACE(NEW.code, '[^a-z0-9_]', '', 'g');
    
    -- Se dopo la normalizzazione il code è vuoto, solleva un errore
    IF NEW.code = '' THEN
        RAISE EXCEPTION 'Il code non può essere vuoto dopo la normalizzazione';
    END IF;
    
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Trigger per ogni tabella vocabolario
DROP TRIGGER IF EXISTS normalize_core_skill_code ON vocab_core_skill;
CREATE TRIGGER normalize_core_skill_code
    BEFORE INSERT OR UPDATE OF code ON vocab_core_skill
    FOR EACH ROW
    EXECUTE FUNCTION normalize_vocab_code();

DROP TRIGGER IF EXISTS normalize_task_code ON vocab_task;
CREATE TRIGGER normalize_task_code
    BEFORE INSERT OR UPDATE OF code ON vocab_task
    FOR EACH ROW
    EXECUTE FUNCTION normalize_vocab_code();

DROP TRIGGER IF EXISTS normalize_language_code ON vocab_language;
CREATE TRIGGER normalize_language_code
    BEFORE INSERT OR UPDATE OF code ON vocab_language
    FOR EACH ROW
    EXECUTE FUNCTION normalize_vocab_code();

DROP TRIGGER IF EXISTS normalize_chat_type_code ON vocab_chat_type;
CREATE TRIGGER normalize_chat_type_code
    BEFORE INSERT OR UPDATE OF code ON vocab_chat_type
    FOR EACH ROW
    EXECUTE FUNCTION normalize_vocab_code();

DROP TRIGGER IF EXISTS normalize_dataset_card_type_code ON vocab_split;
CREATE TRIGGER normalize_dataset_card_type_code
    BEFORE INSERT OR UPDATE OF code ON vocab_split
    FOR EACH ROW
    EXECUTE FUNCTION normalize_vocab_code();

-- Trigger function per mapping history 
CREATE OR REPLACE FUNCTION update_mapping_history()
RETURNS TRIGGER AS $$
BEGIN
    -- Salva nella history solo se il mapping principale è cambiato
    IF OLD.mapping IS DISTINCT FROM NEW.mapping THEN
        INSERT INTO mapping_history 
            (mapping_id, schema_template_id, mapping, version, modified_at)
        VALUES 
            (OLD.id, OLD.schema_template_id, OLD.mapping, OLD.version, NOW());
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Trigger per mapping dello schema_template 
DROP TRIGGER IF EXISTS mapping_update_trigger ON mapping;
CREATE TRIGGER mapping_update_trigger
BEFORE UPDATE OF mapping
ON mapping
FOR EACH ROW
EXECUTE FUNCTION update_mapping_history();

-- Funzione che intercetta l'eliminazione di system prompt
CREATE OR REPLACE FUNCTION fn_soft_delete_system_prompt()
RETURNS TRIGGER AS $$
BEGIN
    -- Se il record è già segnato come eliminato, non fare nulla
    -- Altrimenti, aggiorna lo stato e annulla la cancellazione fisica
    UPDATE system_prompt 
    SET deleted = TRUE, 
        modified = NOW()
    WHERE id = OLD.id;
    
    RETURN NULL; -- Restituendo NULL, PostgreSQL annulla l'operazione di DELETE fisica
END;
$$ LANGUAGE plpgsql;

-- Applicazione del Trigger alla tabella
CREATE TRIGGER tr_soft_delete_system_prompt
BEFORE DELETE ON system_prompt
FOR EACH ROW
EXECUTE FUNCTION fn_soft_delete_system_prompt();



-- Trigger function per system_prompt history 
CREATE OR REPLACE FUNCTION update_system_prompt_history()
RETURNS TRIGGER AS $$
BEGIN
    -- Salva nella history solo se i campi RILEVANTI sono cambiati
    IF OLD.prompt IS DISTINCT FROM NEW.prompt THEN
        INSERT INTO system_prompt_history 
            (system_prompt_id, name, description, prompt, _lang , version,length, issued, modified)
        VALUES 
            (OLD.id, OLD.name, OLD.description, OLD.prompt, OLD._lang, OLD.version, OLD.length, OLD.issued, OLD.modified);
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;
-- Trigger per system_prompt (solo sul campo prompt)
DROP TRIGGER IF EXISTS system_prompt_update_trigger ON system_prompt;
CREATE TRIGGER system_prompt_update_trigger
BEFORE UPDATE OF prompt
ON system_prompt
FOR EACH ROW
EXECUTE FUNCTION update_system_prompt_history();

-- Strategy history trigger function
CREATE OR REPLACE FUNCTION log_strategy_changes()
RETURNS TRIGGER AS $$
BEGIN
    IF (TG_OP = 'UPDATE') THEN
        -- Logghiamo solo se i dati rilevanti sono cambiati
        IF ROW(OLD.replication_factor, OLD.template_strategy, OLD.distribution_id) 
           IS DISTINCT FROM ROW(NEW.replication_factor, NEW.template_strategy, NEW.distribution_id) THEN
           
            INSERT INTO history_strategy (
                original_strategy_id, recipe_id, distribution_id, 
                replication_factor, template_strategy, operation_type
            )
            VALUES (OLD.id, OLD.recipe_id, OLD.distribution_id, 
                    OLD.replication_factor, OLD.template_strategy, 'UPDATE');
        END IF;
        RETURN NEW;
        
    ELSIF (TG_OP = 'DELETE') THEN
        INSERT INTO history_strategy (
            original_strategy_id, recipe_id, distribution_id, 
            replication_factor, template_strategy, operation_type
        )
        VALUES (OLD.id, OLD.recipe_id, OLD.distribution_id, 
                OLD.replication_factor, OLD.template_strategy, 'DELETE');
        RETURN OLD;
    END IF;
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_strategy_history
BEFORE UPDATE OR DELETE ON strategy
FOR EACH ROW EXECUTE FUNCTION log_strategy_changes();

-- strategy system prompt history trigger function
CREATE OR REPLACE FUNCTION log_strategy_prompt_changes()
RETURNS TRIGGER AS $$
BEGIN
    -- In una tabella di mapping, ci interessa sapere quando un'associazione viene rimossa
    IF (TG_OP = 'DELETE') THEN
        INSERT INTO history_strategy_system_prompt (
            original_strategy_id, system_prompt_name, operation_type
        )
        VALUES (OLD.strategy_id, OLD.system_prompt_name, 'DELETE');
        RETURN OLD;
    ELSIF (TG_OP = 'UPDATE') THEN
        INSERT INTO history_strategy_system_prompt (
            original_strategy_id, system_prompt_name, operation_type
        )
        VALUES (OLD.strategy_id, OLD.system_prompt_name, 'UPDATE_MAPPING');
        RETURN NEW;
    END IF;
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_strategy_prompt_history
BEFORE UPDATE OR DELETE ON strategy_system_prompt
FOR EACH ROW EXECUTE FUNCTION log_strategy_prompt_changes();

-- Trigger function per schema_template history 
CREATE OR REPLACE FUNCTION update_schema_template_history()
RETURNS TRIGGER AS $$
BEGIN
    -- Salva nella history solo se lo schema è cambiato
    IF OLD."schema" IS DISTINCT FROM NEW."schema" THEN
        INSERT INTO schema_template_history 
            (schema_template_id, schema, version, modified_at)
        VALUES 
            (OLD.id, OLD."schema", OLD.version, NOW());
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;
-- Trigger per schema_template (solo sul campo schema)
DROP TRIGGER IF EXISTS schema_template_update_trigger ON schema_template;
CREATE TRIGGER schema_template_update_trigger
BEFORE UPDATE OF "schema"
ON schema_template
FOR EACH ROW
EXECUTE FUNCTION update_schema_template_history();

-- Funzione trigger per aggiornare il campo modified in dataset
CREATE OR REPLACE FUNCTION update_modified()
RETURNS TRIGGER AS $$
BEGIN
    NEW.modified = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS dataset_modified_trigger ON dataset;
CREATE TRIGGER dataset_modified_trigger
    BEFORE UPDATE ON dataset
    FOR EACH ROW
    EXECUTE FUNCTION update_modified();

DROP TRIGGER IF EXISTS distribution_modified_trigger ON distribution;
CREATE TRIGGER distribution_modified_trigger
    BEFORE UPDATE ON distribution
    FOR EACH ROW
    EXECUTE FUNCTION update_modified();
    
DROP TRIGGER IF EXISTS recipe_modified_trigger ON recipe;
CREATE TRIGGER recipe_modified_trigger
    BEFORE UPDATE ON recipe
    FOR EACH ROW
    EXECUTE FUNCTION update_modified();

DROP TRIGGER IF EXISTS strategy_modified_trigger ON strategy;
CREATE TRIGGER strategy_modified_trigger
    BEFORE UPDATE ON strategy
    FOR EACH ROW
    EXECUTE FUNCTION update_modified();

DROP TRIGGER IF EXISTS checkpoint_modified_trigger ON checkpoint;
CREATE TRIGGER checkpoint_modified_trigger
    BEFORE UPDATE ON checkpoint
    FOR EACH ROW
    EXECUTE FUNCTION update_modified();

-- Trigger per  validare languages from vocab_language
CREATE OR REPLACE FUNCTION validate_languages()
RETURNS TRIGGER AS $$
DECLARE
    invalid_lang TEXT;
BEGIN
    -- unnest funziona per entrambi perché le colonne si chiamano 'languages' in entrambe le tabelle
    SELECT lang INTO invalid_lang
    FROM unnest(NEW.languages) lang
    WHERE NOT EXISTS (
        SELECT 1 FROM vocab_language WHERE code = lang
    )
    LIMIT 1;
    
    IF invalid_lang IS NOT NULL THEN
        RAISE EXCEPTION 'Linguaggio non valido in %: %', TG_TABLE_NAME, invalid_lang;
    END IF;
    
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Trigger con nomi univoci per ogni tabella
DROP TRIGGER IF EXISTS validate_languages_dataset_card_trigger ON dataset_card;
CREATE TRIGGER validate_languages_dataset_card_trigger
    BEFORE INSERT OR UPDATE ON dataset_card
    FOR EACH ROW
    EXECUTE FUNCTION validate_languages();

DROP TRIGGER IF EXISTS validate_languages_dataset_trigger ON dataset;
CREATE TRIGGER validate_languages_dataset_trigger
    BEFORE INSERT OR UPDATE ON dataset
    FOR EACH ROW
    EXECUTE FUNCTION validate_languages();

-- Funzione trigger per auto-incrementare la versione
CREATE OR REPLACE FUNCTION increment_version(current_version TEXT)
RETURNS TEXT AS $$
DECLARE
    version_parts TEXT[];
    major INT;
    minor INT;
    patch INT;
BEGIN
    -- Se la versione corrente è NULL, restituisci '1.0.0'
    IF current_version IS NULL THEN
        RETURN '1.0.0';
    END IF;
    
    -- Parsifica la versione corrente
    version_parts := string_to_array(current_version, '.');
    
    -- Validazione: almeno una parte deve essere numerica
    IF array_length(version_parts, 1) IS NULL OR 
       version_parts[1] !~ '^\d+$' THEN
        RETURN '1.0.0';  -- Fallback per versioni malformate
    END IF;
    
    -- Estrai parti con validazione
    major := COALESCE(NULLIF(version_parts[1], '')::INT, 1);
    minor := COALESCE(NULLIF(version_parts[2], '')::INT, 0);
    patch := COALESCE(NULLIF(version_parts[3], '')::INT, 0);
    
    -- Ignora parti extra oltre la terza
    patch := patch + 1;
    
    RETURN major::TEXT || '.' || minor::TEXT || '.' || patch::TEXT;
EXCEPTION
    WHEN OTHERS THEN
        -- In caso di qualsiasi errore, restituisci una versione di default
        RETURN '1.0.0';
END;
$$ LANGUAGE plpgsql;

-- funzione auto_increment_version versione semplificata, evita concorrenza
CREATE OR REPLACE FUNCTION auto_increment_version()
RETURNS TRIGGER AS $$
BEGIN
    -- Se è un UPDATE, incrementa SEMPRE la versione
    -- Questo garantisce zero errori di concorrenza e storia completa
    IF TG_OP = 'UPDATE' THEN
        NEW.version = increment_version(OLD.version::TEXT);
        NEW.modified = NOW();
    END IF;
    
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Trigger per auto-incremento versione
DROP TRIGGER IF EXISTS auto_increment_version_dataset_trigger ON dataset;
CREATE TRIGGER auto_increment_version_dataset_trigger
    BEFORE UPDATE ON dataset
    FOR EACH ROW
    EXECUTE FUNCTION auto_increment_version();

DROP TRIGGER IF EXISTS auto_increment_version_distribution_trigger ON distribution;
CREATE TRIGGER auto_increment_version_distribution_trigger
    BEFORE UPDATE ON distribution
    FOR EACH ROW
    EXECUTE FUNCTION auto_increment_version();

DROP TRIGGER IF EXISTS auto_increment_version_system_prompt_trigger ON system_prompt;
CREATE TRIGGER auto_increment_version_system_prompt_trigger
    BEFORE UPDATE ON system_prompt
    FOR EACH ROW
    EXECUTE FUNCTION auto_increment_version();

DROP TRIGGER IF EXISTS auto_increment_version_schema_template_trigger ON schema_template;
CREATE TRIGGER auto_increment_version_schema_template_trigger
    BEFORE UPDATE ON schema_template
    FOR EACH ROW
    EXECUTE FUNCTION auto_increment_version();

DROP TRIGGER IF EXISTS auto_increment_version_mapping_trigger ON mapping;
CREATE TRIGGER auto_increment_version_mapping_trigger
    BEFORE UPDATE ON mapping
    FOR EACH ROW
    EXECUTE FUNCTION auto_increment_version();

CREATE TRIGGER auto_increment_version_recipe_trigger
    BEFORE UPDATE ON recipe
    FOR EACH ROW
    EXECUTE FUNCTION auto_increment_version();

CREATE TRIGGER auto_increment_version_checkpoint_trigger
    BEFORE UPDATE ON checkpoint
    FOR EACH ROW
    EXECUTE FUNCTION auto_increment_version();

-- Trigger Function per validare core_skills
CREATE OR REPLACE FUNCTION validate_core_skills()
RETURNS TRIGGER AS $$
DECLARE
    invalid_skill TEXT;
BEGIN
    -- Per dataset_card
    IF TG_TABLE_NAME = 'dataset_card' THEN
        -- Se l'array non è vuoto, valida ogni elemento
        IF NEW.core_skills IS NOT NULL AND array_length(NEW.core_skills, 1) > 0 THEN
            SELECT skill INTO invalid_skill
            FROM unnest(NEW.core_skills) skill
            WHERE NOT EXISTS (
                SELECT 1 FROM vocab_core_skill WHERE code = skill
            )
            LIMIT 1;
            
            IF invalid_skill IS NOT NULL THEN
                RAISE EXCEPTION 'Core skill non valida: %', invalid_skill;
            END IF;
        END IF;
    END IF;
    
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Trigger Function per validare tasks
CREATE OR REPLACE FUNCTION validate_tasks()
RETURNS TRIGGER AS $$
DECLARE
    invalid_task TEXT;
BEGIN
    -- Per dataset_card
    IF TG_TABLE_NAME = 'dataset_card' THEN
        -- Se l'array non è vuoto, valida ogni elemento
        IF NEW.tasks IS NOT NULL AND array_length(NEW.tasks, 1) > 0 THEN
            SELECT task INTO invalid_task
            FROM unnest(NEW.tasks) task
            WHERE NOT EXISTS (
                SELECT 1 FROM vocab_task WHERE code = task
            )
            LIMIT 1;
            
            IF invalid_task IS NOT NULL THEN
                RAISE EXCEPTION 'Task non valido: %', invalid_task;
            END IF;
        END IF;
    END IF;
    
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Trigger per core_skills su dataset_card
DROP TRIGGER IF EXISTS validate_core_skills_dataset_card_trigger ON dataset_card;
CREATE TRIGGER validate_core_skills_dataset_card_trigger
    BEFORE INSERT OR UPDATE OF core_skills ON dataset_card
    FOR EACH ROW
    EXECUTE FUNCTION validate_core_skills();

-- Trigger per tasks su dataset_card
DROP TRIGGER IF EXISTS validate_tasks_dataset_card_trigger ON dataset_card;
CREATE TRIGGER validate_tasks_dataset_card_trigger
    BEFORE INSERT OR UPDATE OF tasks ON dataset_card
    FOR EACH ROW
    EXECUTE FUNCTION validate_tasks();





-- *****************************************************************************
-- 1. FUNZIONE GENERALE DI VALIDAZIONE STEP (Ontology Kernel)
-- *****************************************************************************
CREATE OR REPLACE FUNCTION fn_validate_step_lineage(
    entity_name TEXT,
    current_step SMALLINT,
    parent_step SMALLINT
) RETURNS VOID AS $$
BEGIN
    -- Se non c'è un genitore (radice), il record è sempre valido
    IF parent_step IS NULL THEN
        RETURN;
    END IF;

    -- Regola: Non è permesso il "downgrade" di maturità (es. Step 2 -> Step 1)
    -- Se current_step == parent_step -> 'is_refinement_of' (OK)
    -- Se current_step > parent_step  -> 'is_transformation_of' (OK)
    IF current_step < parent_step THEN
        RAISE EXCEPTION 'Violazione Ontologica [%]: Lo step corrente (%) non può essere inferiore allo step del genitore (%)', 
            entity_name, current_step, parent_step;
    END IF;
END;
$$ LANGUAGE plpgsql;

-- *****************************************************************************
-- 2. TRIGGER PER LA TABELLA: dataset
-- *****************************************************************************
CREATE OR REPLACE FUNCTION trg_dataset_lineage_check()
RETURNS TRIGGER AS $$
DECLARE
    p_step SMALLINT;
BEGIN
    IF NEW.derived_dataset IS NOT NULL THEN
        -- Recupera lo step del dataset padre tramite ID
        SELECT step INTO p_step FROM dataset WHERE id = NEW.derived_dataset;
        PERFORM fn_validate_step_lineage('DATASET', NEW.step, p_step);
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_validate_dataset_steps ON dataset;
CREATE TRIGGER trg_validate_dataset_steps 
    BEFORE INSERT OR UPDATE ON dataset 
    FOR EACH ROW EXECUTE FUNCTION trg_dataset_lineage_check();
    
-- *****************************************************************************
-- 3. TRIGGER PER LA TABELLA: distribution
-- *****************************************************************************
-- Gestisce sia il lignaggio tra distribution che l'allineamento con il dataset
CREATE OR REPLACE FUNCTION trg_distribution_logic_check()
RETURNS TRIGGER AS $$
DECLARE
    p_dist_step SMALLINT;
    p_ds_step   SMALLINT;
BEGIN
    -- A. Verifica Allineamento con il Dataset di appartenenza (Verticale)
    SELECT step INTO p_ds_step FROM dataset WHERE id = NEW.dataset_id;
    
    IF NEW.step <> p_ds_step THEN
        RAISE EXCEPTION 'Violazione Allineamento: La Distribution (Step %) deve avere lo stesso Step del Dataset padre (ID: %, Step: %)', 
            NEW.step, NEW.dataset_id, p_ds_step;
    END IF;

    -- B. Verifica Lignaggio con la Distribution sorgente (Orizzontale/Diagonale)
    IF NEW.derived_from IS NOT NULL THEN
        SELECT step INTO p_dist_step FROM distribution WHERE id = NEW.derived_from;
        PERFORM fn_validate_step_lineage('DISTRIBUTION', NEW.step, p_dist_step);
    END IF;

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_validate_distribution_all
BEFORE INSERT OR UPDATE ON distribution
FOR EACH ROW EXECUTE FUNCTION trg_distribution_logic_check();

-- *****************************************************************************
-- 4. TRIGGER PER LA TABELLA: strategy
-- *****************************************************************************
-- Garantisce che solo i dati "Gold" (Step 3) entrino nel processo di training
CREATE OR REPLACE FUNCTION trg_strategy_step_check()
RETURNS TRIGGER AS $$
DECLARE
    dist_step SMALLINT;
BEGIN
    SELECT step INTO dist_step FROM distribution WHERE id = NEW.distribution_id;

    IF dist_step != 3 THEN
        RAISE EXCEPTION 'Violazione Strategia: Solo le distribution di Step 3 possono essere usate in una Recipe (Dist ID: %, Step: %)', 
            NEW.distribution_id, dist_step;
    END IF;

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_validate_strategy_eligibility
BEFORE INSERT OR UPDATE ON strategy
FOR EACH ROW EXECUTE FUNCTION trg_strategy_step_check();


CREATE OR REPLACE FUNCTION trg_protect_card_integrity()
RETURNS TRIGGER AS $$
DECLARE
    exists_flag BOOLEAN;
BEGIN
    -- Try to check for dependent datasets in the same schema as the trigger.
    BEGIN
        EXECUTE format('SELECT EXISTS (SELECT 1 FROM %I.%I WHERE derived_card = $1)', TG_TABLE_SCHEMA, 'dataset')
        USING OLD.id INTO exists_flag;
    EXCEPTION WHEN undefined_table THEN
        -- If the dataset table doesn't exist in this schema, treat as no dependencies.
        exists_flag := FALSE;
    END;

    IF exists_flag THEN
        RAISE EXCEPTION 'Impossibile eliminare DatasetCard: esistono Dataset attivi che dipendono da questa ontologia.';
    END IF;
    RETURN OLD;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_card_delete_protection
BEFORE DELETE ON dataset_card
FOR EACH ROW EXECUTE FUNCTION trg_protect_card_integrity();





-- SKILL/TASK coherence trigger function ####

-- 1. Funzione di Validazione
CREATE OR REPLACE FUNCTION fn_validate_dataset_skill_task_coherence()
RETURNS TRIGGER AS $$
DECLARE
    s_code TEXT;
    is_valid BOOLEAN;
    has_mix_skill BOOLEAN;
    has_mix_task BOOLEAN;
BEGIN
    -- STEP 1: Check preliminari per array vuoti
    -- Se non ci sono skill, passiamo (accettiamo dataset con sola indicazione del task)
    IF NEW.core_skills IS NULL OR array_length(NEW.core_skills, 1) = 0 THEN
        RETURN NEW;
    END IF;

    -- STEP 2: Gestione Logica mix (Bypass)
    has_mix_skill := 'mix' = ANY(NEW.core_skills);
    has_mix_task := NEW.tasks IS NOT NULL AND 'mix' = ANY(NEW.tasks);

    -- Se una delle due parti è mix, la coerenza è garantita per definizione simmetrica
    IF has_mix_skill OR has_mix_task THEN
        RETURN NEW;
    END IF;

    -- Se ci sono skill ma mancano i task (e non siamo in caso mix), solleviamo errore
    -- Una skill deve sempre essere ancorata a un esercizio pratico (Task)
    IF NEW.tasks IS NULL OR array_length(NEW.tasks, 1) = 0 THEN
        RAISE EXCEPTION 'Coherence Error: Hai dichiarato delle Skill specifiche (%) senza indicare alcun Task. Seleziona i task coerenti o usa mix.', 
        array_to_string(NEW.core_skills, ', ');
    END IF;

    -- STEP 3: Validazione Iterativa (Ogni Skill deve aderire ad almeno un Task selezionato)
    FOREACH s_code IN ARRAY NEW.core_skills
    LOOP
        -- Cerchiamo se esiste almeno una riga nella tassonomia che leghi la skill corrente
        -- a uno qualsiasi dei task inseriti nella dataset_card
        SELECT EXISTS (
            SELECT 1 
            FROM skill_task_taxonomy 
            WHERE skill_code = s_code 
            AND task_code = ANY(NEW.tasks)
        ) INTO is_valid;

        IF NOT is_valid THEN
            RAISE EXCEPTION 'Incoerenza Tassonomica: La skill "%" non è ammissibile per i task selezionati (%). Controlla la skill_task_taxonomy o usa la skill mix.', 
            s_code, array_to_string(NEW.tasks, ', ');
        END IF;
    END LOOP;

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- 2. Trigger applicato alla tabella dataset_card
CREATE TRIGGER trg_validate_dataset_card_coherence
BEFORE INSERT OR UPDATE ON dataset_card
FOR EACH ROW EXECUTE FUNCTION fn_validate_dataset_skill_task_coherence();
