-- Inserimento source_category
INSERT INTO vocab_source_category (code, description) VALUES
('web', 'Content sourced from web infrastructure: websites, web apps, online platforms'),
('academic', 'Formally published materials: journals, academic papers, reports'), 
('code', 'Organized digital collections: code repos, data archives, institutional repositories'), 
('government', 'Official governmental and public authority sources'),
('edu', 'Educational institutions and learning platforms'),  
('legal', 'Legal systems, courts, and jurisprudence databases'),
('encyclopedic', 'Reference knowledge bases and encyclopedic sources'),  
('misc', 'Uncategorized or multi-domain sources'),
('book', 'Content extracted or derived from books and monographs'),
('news', 'Journalistic content from newspapers, media outlets, and news agencies');

-- Inserimento source_type
INSERT INTO vocab_source_type (code, description) VALUES
-- 1. Origine/Ownership
('public', 'Publicly accessible without restrictions or authentication'),
('open', 'Released under open licenses (e.g., Creative Commons, MIT)'),  
('internal', 'Generated or maintained internally by the organization'),
('customer', 'Directly provided by customers or end users'),
('partner', 'Shared by strategic or commercial partners'),
('vendor', 'Supplied by third-party vendors or service providers'),
('licensed', 'Obtained through commercial licensing agreements'),  
-- 2. Generazione/Processamento
('collected', 'Gathered from existing sources without modification'),  
('scraped', 'Extracted via automated web scraping'),  
('crowdsourced', 'Contributed by distributed communities of volunteers'), 
('curated', 'Manually reviewed, cleaned, and organized by experts'),
('annotated', 'Enriched with labels, tags, or human-generated metadata'),
('derived', 'Produced through transformation or aggregation of other datasets'),
('synthetic', 'Artificially generated data not from real-world observations'),
('generated', 'Automatically created by algorithms or ML models (e.g., LLM outputs)'),
('donated', 'Voluntarily contributed by individuals or organizations');  

-- Inserimento field
INSERT INTO vocab_field (code, description) VALUES
('law', 'Legal studies including legislation, case law, and regulatory frameworks'),
('mathematics', 'Mathematical theory, proofs, models, and quantitative methods'),
('computer_science', 'Computing theory, software systems, and algorithmic research'),
('physics', 'Theoretical and experimental study of physical phenomena'),
('biology', 'Life sciences including molecular, cellular, and ecological biology'),
('chemistry', 'Chemical sciences covering substances, reactions, and materials'),
('economics', 'Economic theory, markets, and quantitative economic analysis'),
('finance', 'Financial systems, instruments, markets, and risk analysis'),
('healthcare', 'Medical, clinical, and healthcare-related knowledge and data'),
('engineering', 'Applied engineering disciplines and technological systems'),
('education', 'Pedagogy, learning sciences, and educational methodologies'),
('linguistics', 'Study of language structure, semantics, and communication'),
('philosophy', 'Philosophical inquiry including logic, ethics, and epistemology'),
('social_science', 'Social and behavioral sciences studying human society and interactions'),
('psychology', 'Psychological sciences studying behavior and mental processes'),
('environmental_science', 'Environmental systems, climate science, and sustainability'),
('history', 'Historical studies and archival research');

-- Inserimento vertical
INSERT INTO vocab_vertical (code, description) VALUES
('enterprise', 'Large-scale organizations with complex operational and data needs'),
('corporate', 'Commercial companies operating in traditional corporate environments'),
('b2b', 'Business-to-business contexts and enterprise services'),
('consumer', 'Products and services targeting individual consumers'),
('public_sector', 'Governmental and public administration use cases'),
('research', 'Scientific and industrial research environments'),
('education', 'Academic institutions and educational organizations'),
('regulated_industry', 'Industries subject to strict regulatory and compliance requirements'),
('non_profit', 'Non-profit and mission-driven organizations'),
('startup', 'Early-stage companies focused on innovation and rapid growth');

-- Inserimneto content
INSERT INTO vocab_content (code, description) VALUES
('contracts', 'Legally binding agreements defining rights and obligations between parties'),
('legal_documents', 'Formal legal texts including filings, rulings, and statutory documents'),
('agreements', 'Written arrangements outlining mutual commitments between entities'),
('clauses', 'Individual provisions or sections within legal or contractual documents'),
('policies', 'Organizational or institutional rules governing behavior and decisions'),
('regulations', 'Authoritative rules issued by governmental or regulatory bodies'),
('proofs', 'Formal mathematical demonstrations establishing the validity of statements'),
('theorems', 'Established propositions proven within a formal mathematical system'),
('equations', 'Mathematical expressions representing relationships between quantities'),
('problem_sets', 'Collections of structured problems used for training or evaluation'),
('derivations', 'Step-by-step logical or mathematical transformations leading to results'),
('algorithms', 'Well-defined computational procedures for solving problems'),
('source_code', 'Human-readable program instructions written in programming languages'),
('technical_docs', 'Technical documentation describing systems, architectures, or software'),
('api_specs', 'Formal specifications defining application programming interfaces'),
('research_papers', 'Scholarly articles reporting original research and findings'),
('datasets', 'Structured collections of data intended for analysis or model training'),
('experimental_logs', 'Recorded outputs and observations from experimental processes'),
('clinical_notes', 'Unstructured or semi-structured notes produced in clinical settings'),
('case_reports', 'Detailed descriptions of individual clinical or operational cases'),
('financial_statements', 'Formal records describing the financial activities of an entity'),
('reports', 'Structured analytical documents summarizing findings or performance'),
('spreadsheets', 'Tabular data files organized in rows and columns'),
('tables', 'Structured tabular representations of data'),
('structured_records', 'Highly structured data entries following a predefined schema'),
('dialogues', 'Conversational exchanges between two or more participants'),
('instructions', 'Step-by-step directives describing how to perform tasks'),
('tutorials', 'Educational materials designed to teach concepts or skills'),
('educational_materials', 'Learning resources created for instructional or training purposes'),
('articles', 'Journalistic or editorial articles from media outlets'),
('books', 'Full-length published books or monographs'),
('forum_posts', 'User-generated content from discussion forums and Q&A sites'),
('social_media_posts', 'Content from social networking platforms'),
('emails', 'Email communications and correspondence'),
('transcripts', 'Written records of spoken dialogue or proceedings'),
('annotations', 'Metadata labels or tags added to enrich primary content'),
('summaries', 'Condensed versions or abstracts of longer documents'),
('translations', 'Content translated from one language to another');

----------------------------

-- Inserimento dataset modalities
INSERT INTO
    vocab_modality (code, description, mime)
VALUES (
        'text',
        'Textual data modality',
        '{text/plain, text/html, text/css, text/csv, application/json, application/xml}'
    ),
    (
        'image',
        'Image data modality',
        '{image/jpeg, image/png, image/gif, image/webp, image/svg+xml}'
    ),
    (
        'audio',
        'Audio data modality',
        '{audio/mpeg, audio/wav, audio/ogg, audio/aac, audio/flac}'
    ),
    (
        'video',
        'Video data modality',
        '{video/mp4, video/webm, video/ogg, video/x-msvideo}'
    ),
    (
        'tabular',
        'Tabular data modality',
        '{text/csv, application/vnd.ms-excel, application/vnd.openxmlformats-officedocument.spreadsheetml.sheet, application/json}'
    ),
    (
        'time_series',
        'Time series data modality',
        '{text/csv, application/json, application/x-parquet}'
    ),
    (
        'multimodal',
        'Multiple data modalities combined',
        '{multipart/form-data, multipart/mixed}'
    );

-- Inserimento task
INSERT INTO vocab_task (code, description) VALUES 
('text_generation', 'Generazione creativa o libera di testo da prompt aperti'),
('instruction_following', 'Esecuzione di output basata su istruzioni e vincoli espliciti'),
('question_answering', 'Risposta a domande (aperte/chiuse) basate su contesto o conoscenza'),
('multiple_choice_qa', 'Risposta a domande tramite selezione di opzioni predefinite (MCQ)'),
('summarization', 'Produzione di sintesi preservando le informazioni chiave'),
('translation', 'Traduzione e localizzazione tra diverse lingue naturali'),
('text_transformation', 'Riscrittura, parafrasi, anonimizzazione o cambio di registro'),
('classification', 'Assegnazione di etichette, categorie o sentiment analysis'),
('information_extraction', 'Estrazione di entità, relazioni o dati strutturati (JSON/XML)'),
('code_generation', 'Scrittura, debugging o completamento di codice e script'),
('code_execution', 'Simulazione o validazione di runtime del codice generato'),
('mathematical_problem_solving', 'Risoluzione di problemi che richiedono calcoli o formule'),
('conversational_dialogue', 'Gestione di contesti e persona su più turni interattivi'),
('tool_interaction', 'Uso di API, Function Calling o sistemi RAG'),
('logic_evaluation', 'Verifica di relazioni logiche, implicazioni e NLI'),
('mix', 'Task multipli o dataset generalisti - abilita tutte le combinazioni');

-- Inserimento core skills
INSERT INTO vocab_core_skill (code, description) VALUES 
('logical_reasoning', 'Capacità di eseguire inferenze strutturate, formali e deduttive'),
('mathematical_reasoning', 'Capacità di manipolare simboli numerici e logica aritmetica'),
('procedural_planning', 'Capacità di decomporre obiettivi in step sequenziali'),
('knowledge_recall', 'Richiamo di fatti e concetti dalla memoria parametrica'),
('linguistic_proficiency', 'Padronanza profonda di sintassi, semantica e registri'),
('commonsense_reasoning', 'Applicazione di logica del mondo e norme sociali'),
('context_management', 'Mantenimento della coerenza su finestre di contesto ampie'),
('creative_synthesis', 'Generazione di connessioni originali o contenuti immaginativi'),
('empathetic_modeling', 'Riconoscimento e simulazione di toni emotivi'),
('symbolic_logic_understanding', 'Comprensione di strutture algoritmiche e linguaggi formali'),
('pattern_structuring_abstraction', 'Identificazione e organizzazione di pattern in dati grezzi'),
('multimodal_integration', 'Sintesi di informazioni tra diverse modalità (es. immagini/testo)'),
('instruction_adherence', 'Disciplina nel rispettare rigorosamente i vincoli del prompt'),
('mix', 'Skill multiple o non isolabili - abilita tutte le combinazioni');

-- Inserimento tassonomia task-skill
INSERT INTO skill_task_taxonomy (task_code, skill_code) VALUES 
-- Core Mappings
('text_generation', 'linguistic_proficiency'), ('text_generation', 'creative_synthesis'),
('instruction_following', 'instruction_adherence'), ('instruction_following', 'procedural_planning'),
('question_answering', 'knowledge_recall'), ('question_answering', 'linguistic_proficiency'),('question_answering', 'logical_reasoning'),
('multiple_choice_qa', 'logical_reasoning'), ('multiple_choice_qa', 'knowledge_recall'),
('summarization', 'linguistic_proficiency'), ('summarization', 'pattern_structuring_abstraction'),
('translation', 'linguistic_proficiency'),
('text_transformation', 'linguistic_proficiency'), ('text_transformation', 'empathetic_modeling'),
('classification', 'logical_reasoning'), ('classification', 'pattern_structuring_abstraction'),
('information_extraction', 'pattern_structuring_abstraction'), ('information_extraction', 'linguistic_proficiency'),
('code_generation', 'symbolic_logic_understanding'), ('code_generation', 'procedural_planning'),
('code_execution', 'symbolic_logic_understanding'),
('mathematical_problem_solving', 'mathematical_reasoning'), ('mathematical_problem_solving', 'procedural_planning'),
('conversational_dialogue', 'context_management'), ('conversational_dialogue', 'empathetic_modeling'),('conversational_dialogue', 'commonsense_reasoning'),
('tool_interaction', 'instruction_adherence'), ('tool_interaction', 'procedural_planning'),
('logic_evaluation', 'logical_reasoning'), ('logic_evaluation', 'commonsense_reasoning'),('logic_evaluation', 'linguistic_proficiency'),
-- JOLLY LOGIC (Simmetrica)
('mix', 'logical_reasoning'), ('mix', 'mathematical_reasoning'), ('mix', 'procedural_planning'),
('mix', 'knowledge_recall'), ('mix', 'linguistic_proficiency'), ('mix', 'commonsense_reasoning'),
('mix', 'context_management'), ('mix', 'creative_synthesis'), ('mix', 'empathetic_modeling'),
('mix', 'symbolic_logic_understanding'), ('mix', 'pattern_structuring_abstraction'),
('mix', 'multimodal_integration'), ('mix', 'instruction_adherence'), ('mix', 'mix'),
-- Skill mix abilita tutti i task specifici
('text_generation', 'mix'), ('instruction_following', 'mix'), ('question_answering', 'mix'),
('multiple_choice_qa', 'mix'), ('summarization', 'mix'), ('translation', 'mix'),
('text_transformation', 'mix'), ('classification', 'mix'), ('information_extraction', 'mix'),
('code_generation', 'mix'), ('code_execution', 'mix'), ('mathematical_problem_solving', 'mix'),
('conversational_dialogue', 'mix'), ('tool_interaction', 'mix'), ('logic_evaluation', 'mix');

-- Inserimento languages
INSERT INTO
    vocab_language (code, description)
VALUES 
    ('un', 'Unknown or unspecified language'),
    ('multi', 'Generical, undefined multi-language content'),
    ('af', 'Afrikaans'),
    ('ar', 'Arabic'),
    ('az', 'Azerbaijani'),
    ('bg', 'Bulgarian'),
    ('bn', 'Bengali'),
    ('ca', 'Catalan'),
    ('cs', 'Czech'),
    ('da', 'Danish'),
    ('de', 'German'),
    ('el', 'Greek'),
    ('en', 'English'),
    ('es', 'Spanish'),
    ('et', 'Estonian'),
    ('fa', 'Persian'),
    ('fi', 'Finnish'),
    ('fr', 'French'),
    ('he', 'Hebrew'),
    ('hi', 'Hindi'),
    ('hr', 'Croatian'),
    ('hu', 'Hungarian'),
    ('id', 'Indonesian'),
    ('it', 'Italian'),
    ('ja', 'Japanese'),
    ('ko', 'Korean'),
    ('lt', 'Lithuanian'),
    ('lv', 'Latvian'),
    ('nl', 'Dutch'),
    ('no', 'Norwegian'),
    ('pl', 'Polish'),
    ('pt', 'Portuguese'),
    ('ro', 'Romanian'),
    ('ru', 'Russian'),
    ('sk', 'Slovak'),
    ('sl', 'Slovenian'),
    ('sq', 'Albanian'),
    ('sr', 'Serbian'),
    ('sv', 'Swedish'),
    ('th', 'Thai'),
    ('tr', 'Turkish'),
    ('uk', 'Ukrainian'),
    ('vi', 'Vietnamese'),
    ('zh', 'Chinese');

-- Inserimento dataset types
INSERT INTO
    vocab_dataset_type (code, description)
VALUES (
        'pretraining',
        'Dataset for foundation model pretraining'
    ),
    (
        'sft',
        'Supervised fine-tuning dataset (input-output pairs)'
    ),
    (
        'preference',
        'Preference or ranking dataset for alignment'
    ),
    (
        'reward_model',
        'Dataset for training reward models'
    ),
    (
        'evaluation',
        'Dataset for model evaluation'
    ),
    (
        'unknown',
        'Unknown or unspecified dataset type'
    );

-- Inserimento distribution splits
INSERT INTO
    vocab_split (code, description)
VALUES ('train', 'Training split'),
    (
        'validation',
        'Validation split'
    ),
    ('test', 'Test split'),
    ('unknown', 'Unknown split');

-- Inserimento licenses
INSERT INTO
    vocab_license (code, description, license_url, note)
VALUES 
    ('mit', 'MIT License', 'https://opensource.org/license/mit', NULL),
    ('apache-2.0', 'Apache License 2.0', 'https://www.apache.org/licenses/LICENSE-2.0', NULL),
    ('bsd-3-clause', 'BSD 3-Clause License', 'https://opensource.org/license/bsd-3-clause', NULL),
    ('bsd-2-clause', 'BSD 2-Clause License', 'https://opensource.org/license/bsd-2-clause', NULL),
    ('gpl-3.0', 'GPL 3.0', 'https://www.gnu.org/licenses/gpl-3.0.txt', NULL),
    ('gpl-2.0', 'GPL 2.0', 'https://www.gnu.org/licenses/old-licenses/gpl-2.0.txt', NULL),
    ('lgpl-3.0', 'LGPL 3.0', 'https://www.gnu.org/licenses/lgpl-3.0.txt', NULL),
    ('lgpl-2.1', 'LGPL 2.1', 'https://www.gnu.org/licenses/old-licenses/lgpl-2.1.txt', NULL),
    ('agpl-3.0', 'AGPL 3.0', 'https://www.gnu.org/licenses/agpl-3.0.txt', NULL),
    ('cc-by-4.0', 'Creative Commons Attribution 4.0 International', 'https://creativecommons.org/licenses/by/4.0/', NULL),
    ('cc-by-sa-4.0', 'Creative Commons Attribution-ShareAlike 4.0 International', 'https://creativecommons.org/licenses/by-sa/4.0/', NULL),
    ('cc-by-nc-4.0', 'Creative Commons Attribution-NonCommercial 4.0 International', 'https://creativecommons.org/licenses/by-nc/4.0/', NULL),
    ('cc-by-nd-4.0', 'Creative Commons Attribution-NoDerivatives 4.0 International', 'https://creativecommons.org/licenses/by-nd/4.0/', NULL),
    ('cc-by-nc-sa-4.0', 'Creative Commons Attribution-NonCommercial-ShareAlike 4.0 International', 'https://creativecommons.org/licenses/by-nc-sa/4.0/', NULL),
    ('cc-by-nc-nd-4.0', 'Creative Commons Attribution-NonCommercial-NoDerivatives 4.0 International', 'https://creativecommons.org/licenses/by-nc-nd/4.0/', NULL),
    ('cc0-1.0', 'Creative Commons Zero v1.0 Universal', 'https://creativecommons.org/publicdomain/zero/1.0/', NULL),
    ('odc-by', 'Open Data Commons Attribution License v1.0', 'https://opendatacommons.org/licenses/by/1-0/', NULL),
    ('odc-pddl', 'Open Data Commons Public Domain Dedication and License', 'https://opendatacommons.org/licenses/pddl/1-0/', NULL),
    ('odbl-1.0', 'Open Database License v1.0', 'https://opendatacommons.org/licenses/odbl/1-0/', NULL),
    ('academic-free-license', 'Academic Free License', 'https://opensource.org/license/afl-3-0-php', NULL),
    ('educational-community-license', 'Educational Community License', 'https://opensource.org/license/ecl-2-0', NULL),
    ('proprietary', 'Proprietary License', NULL, 'General category for closed licenses'),
    ('commercial', 'Commercial License', NULL, 'License for commercial use'),
    ('research-only', 'Research Only License', NULL, 'Usage restricted to research purposes'),
    ('non-commercial', 'Non-Commercial License', NULL, 'Usage restricted to non-commercial purposes'),
    ('mpl-2.0', 'Mozilla Public License 2.0', 'https://www.mozilla.org/en-US/MPL/2.0/', NULL),
    ('epl-2.0', 'Eclipse Public License 2.0', 'https://www.eclipse.org/legal/epl-2.0/', NULL),
    ('cddl-1.0', 'Common Development and Distribution License 1.0', 'https://opensource.org/license/cddl-1-0', NULL),
    ('bigscience-open-rail-m', 'BigScience Open RAIL-M License', 'https://bigscience.huggingface.co/blog/the-bigscience-rail-license', NULL),
    ('bigscience-bloom-rail-1.0', 'BigScience BLOOM RAIL 1.0 License', 'https://huggingface.co/spaces/bigscience/license', NULL),
    ('ai2-impact', 'AI2 ImpACT License', 'https://allenai.org/impact-license', NULL),
    ('public-domain', 'Public Domain', NULL, 'Dedicated to the public domain'),
    ('unlicense', 'The Unlicense', 'https://unlicense.org/', NULL),
    ('unknown', 'License not specified', NULL, NULL),
    ('other', 'Other license type', NULL, NULL),
    -- Integrazione Licenze Obbligatorie
    ('cc-', 'Creative Commons - unspecified', NULL, 'placeholder per CC senza esplicita versione, da approfondire'),
    ('modified-mit', 'Modified MIT License', 'https://github.com/openai/gpt-2/blob/master/LICENSE', NULL),
    ('cc-sa-3-0', 'Creative Commons Attribution-ShareAlike 3.0 Unported', 'https://creativecommons.org/licenses/by-sa/3.0/', NULL),
    ('eupl-1-2', 'European Union Public License 1.2', 'https://interoperable-europe.ec.europa.eu/collection/eupl/eupl-text-eupl-12', 'verificare la versione esatta (poco chiara)'),
    ('common-crawl-limited-license', 'Common Crawl Limited License', 'https://commoncrawl.org/terms-of-use', 'NOTA: fair use'),
    ('gfdl-1-3', 'GNU Free Documentation License v1.3 or later', 'https://www.gnu.org/licenses/fdl-1.3.txt', 'verificare la versione esatta (poco chiara)');
------------------------------------------------------------
-- INSERT DEFAULT RECORDS INTO schema_template
------------------------------------------------------------

-- Pretraining_Template_V1
INSERT INTO
    schema_template (
        "id",
        "name",
        "description",
        "schema",
        "version"
    ) OVERRIDING SYSTEM VALUE
VALUES (
        '3df02834-896d-466c-a082-0274e9254b99',
        'pretraining_schema_template',
        'Schema standard per dataset di pretraining, include campi per percorso dataset, sottopercorso, nome file, lingua, formato, distribuzione e hash identificativo.',
        '{
  "$schema": "http://json-schema.org/draft-07/schema#",
  "type": "object",
  "properties": {
    "text": {
      "type": "string",
      "minLength": 1
    },
    "_dataset_name": {
      "type": "string",
      "minLength": 1
    },
    "_id_hash": {
      "type": "string",
      "pattern": "^[a-fA-F0-9]{64}$"
    },
    "_dataset_path": {
      "type": "string",
      "minLength": 1
    },
    "_subpath": {
      "type": "string",
      "minLength": 1
    },
    "_filename": {
      "type": "string",
      "minLength": 1
    },
    "_lang": {
      "type": "string",
      "maxLength": 2,
      "default": "un"
    }
    },
    "required": [
      "id",
      "text",
      "_dataset_name",
      "_id_hash",
      "_dataset_path",
      "_subpath",
      "_filename",
      "_lang"
    ],
    "additionalProperties": false
 }'::jsonb, -- Cast esplicito a JSONB
        '1.0'
    );

-- SFT_Chat_Template_V1
INSERT INTO
    schema_template (
        "id",
        "name",
        "description",
        "schema",
        "version"
    ) OVERRIDING SYSTEM VALUE
VALUES (
        '3df02834-896d-466c-a082-0274e9254b95',
        'SFT_Chat_Template',
        'Schema standard per conversazioni multi-turno, supporta messaggi, ruoli (USER/ASSISTANT/SYSTEM), contesto (context), ragionamento interno (think) e chiamate a funzioni (functioncall).',
        '{
  "$schema": "http://json-schema.org/draft-07/schema#",
  "type": "object",
  "properties": {
    "system": {
      "type": [
        "string",
        "null"
      ]
    },
    "context": {
      "type": [
        "string",
        "null"
      ]
    },
    "messages": {
      "type": "array",
      "minItems": 1,
      "items": {
        "type": "object",
        "properties": {
          "content": {
            "anyOf": [
              {
                "type": "string",
                "minLength": 1
              },
              {
                "type": "null"
              }
            ]
          },
          "role": {
            "type": "string",
            "enum": [
              "USER",
              "ASSISTANT",
              "SYSTEM"
            ]
          },
          "think": {
            "anyOf": [
              {
                "type": "string",
                "minLength": 1
              },
              {
                "type": "null"
              }
            ]
          },
          "functioncall": {
            "anyOf": [
              {
                "type": "null"
              },
              {
                "type": "object",
                "properties": {
                  "payload": {
                    "type": "string",
                    "minLength": 1
                  },
                  "response": {
                    "type": "string",
                    "minLength": 1
                  }
                },
                "required": [
                  "payload",
                  "response"
                ],
                "additionalProperties": false
              }
            ]
          },
          "context": {
            "type": [
              "string",
              "null"
            ]
          }
        },
        "required": [
          "content",
          "role",
          "think",
          "functioncall",
          "context"
        ],
        "additionalProperties": false
      }
    },
    "_lang": {
      "type": "string",
      "maxLength": 2,
      "default": "un"
    },
    "_dataset_path": {
      "type": "string",
      "minLength": 1
    },
    "_subpath": {
      "type": "string",
      "minLength": 1
    },
    "_filename": {
      "type": "string",
      "minLength": 1
    },
    "_dataset_name": {
      "type": "string",
      "minLength": 1
    },
    "_id_hash": {
      "type": "string",
      "pattern": "^[a-fA-F0-9]{64}$"
    }

  },
  "required": [
    "messages",
    "_lang",
    "_dataset_path",
    "_subpath",
    "_filename",
    "_dataset_name",
    "_id_hash"
  ],
  "additionalProperties": false
}'::jsonb, -- Cast esplicito a JSONB
        '1.0'
    );


-- SFT_Chat_Template_cycle_2
INSERT INTO
    schema_template (
        "id",
        "name",
        "description",
        "schema",
        "version"
    ) OVERRIDING SYSTEM VALUE
VALUES (
        '3df02834-896d-466c-a082-0274e9254b05',
        'SFT_Chat_Template_cycle2',
        '[template embedded] Schema standard per conversazioni multi-turno, supporta messaggi, ruoli (USER/ASSISTANT/SYSTEM), contesto (context), ragionamento interno (think) e chiamate a funzioni (functioncall).',
        '{
  "$schema": "http://json-schema.org/draft-07/schema#",
  "type": "object",
  "properties": {
    "system": {
      "type": [
        "string",
        "null"
      ]
    },
    "template": {
      "type": "string",
      "enum": [
       "fc_chat_st",
        "context_chat",
        "simple_chat",
        "simple_chat_think",
        "simple_context_chat"
      ]
    },
    "context": {
      "type": [
        "string",
        "null"
      ]
    },
    "messages": {
      "type": "array",
      "minItems": 1,
      "items": {
        "type": "object",
        "properties": {
          "content": {
            "anyOf": [
              {
                "type": "string",
                "minLength": 1
              },
              {
                "type": "null"
              }
            ]
          },
          "role": {
            "type": "string",
            "enum": [
              "USER",
              "ASSISTANT",
              "SYSTEM"
            ]
          },
          "think": {
            "anyOf": [
              {
                "type": "string",
                "minLength": 1
              },
              {
                "type": "null"
              }
            ]
          },
          "functioncall": {
            "anyOf": [
              {
                "type": "null"
              },
              {
                "type": "object",
                "properties": {
                  "payload": {
                    "type": "string",
                    "minLength": 1
                  },
                  "response": {
                    "type": "string",
                    "minLength": 1
                  }
                },
                "required": [
                  "payload",
                  "response"
                ],
                "additionalProperties": false
              }
            ]
          },
          "context": {
            "type": [
              "string",
              "null"
            ]
          }
        },
        "required": [
          "content",
          "role",
          "think",
          "functioncall",
          "context"
        ],
        "additionalProperties": false
      }
    },
    "_lang": {
      "type": "string",
      "maxLength": 2,
      "default": "un"
    },
    "_dataset_path": {
      "type": "string",
      "minLength": 1
    },
    "_subpath": {
      "type": "string",
      "minLength": 1
    },
    "_filename": {
      "type": "string",
      "minLength": 1
    },
    "_dataset_name": {
      "type": "string",
      "minLength": 1
    },
    "_id_hash": {
      "type": "string",
      "pattern": "^[a-fA-F0-9]{64}$"
    }
  },
  "required": [
    "messages",
    "template",
    "_lang",
    "_dataset_path",
    "_subpath",
    "_filename",
    "_dataset_name",
    "_id_hash"
  ],
  "additionalProperties": false
}'::jsonb, -- Cast esplicito a JSONB
        '1.0'
    );


-- Inserimento chat types
INSERT INTO
    vocab_chat_type (code, description, schema_id)
VALUES 
    ('pretraining', 'Pretraining text-metadata schema', '3df02834-896d-466c-a082-0274e9254b99'),
    ('simple_chat','Basic single-turn conversation', '3df02834-896d-466c-a082-0274e9254b95'),
    ('context_chat','Conversation with context retention', '3df02834-896d-466c-a082-0274e9254b95'),
    ('simple_chat_think','Single-turn with reasoning steps', '3df02834-896d-466c-a082-0274e9254b95'),
    ('fc_chat_st','Function Calling Chat Static', '3df02834-896d-466c-a082-0274e9254b95'),
    ('simple_context_chat', 'Simplified multi-turn conversation without heavy reasoning', '3df02834-896d-466c-a082-0274e9254b95'),
    ('simple_chat_cycle2','Basic single-turn conversation', '3df02834-896d-466c-a082-0274e9254b05'),
    ('context_chat_cycle2','Conversation with context retention', '3df02834-896d-466c-a082-0274e9254b05'),
    ('simple_chat_think_cycle2','Single-turn with reasoning steps', '3df02834-896d-466c-a082-0274e9254b05'),
    ('fc_chat_st_cycle2','Function Calling Chat Static', '3df02834-896d-466c-a082-0274e9254b05'),
    ('simple_context_chat_cycle2', 'Simplified multi-turn conversation without heavy reasoning', '3df02834-896d-466c-a082-0274e9254b05');


------------------------------------------------------------
-- 1. INSERT INTO dataset_card
------------------------------------------------------------
INSERT INTO
    dataset_card (
        id,
        dataset_name,
        dataset_id,
        modality,
        dataset_description,
        publisher,
        notes,
        source_url,
        download_url,
        languages,
        license,
        core_skills,
        tasks,
        has_reasoning,
        last_update,
        created_at,
        quality
    )
VALUES (
        'e79a8506-fe8e-456e-b5bb-ec3ee2d2a5bb',
        'allenai/ai2_arc',
        'allenai/ai2_arc',
        'text',
        'A new dataset of 7,787 genuine grade-school level, multiple-choice science questions, assembled to encourage research in advanced question-answering. The dataset is partitioned into a Challenge Set and an Easy Set, where the former contains only questions answered incorrectly by both a retrieval-based algorithm and a word co-occurrence algorithm. We are also including a corpus of over 14 million science sentences relevant to the task, and an implementation of three neural baseline models for this dataset. We pose ARC as a challenge to the community.',
        NULL,
        'download HF',
        'https://huggingface.co/datasets/allenai/ai2_arc',
        NULL,
        '{en}',
        'cc-by-sa-4.0',
        -- core_skills coerenti con il task multiple_choice_qa
        '{knowledge_recall}',
        -- task corretto
        '{multiple_choice_qa}',
        FALSE,
        '2026-01-01 00:00:00+00',
        '2026-01-01 00:00:00+00',
        4
    );

------------------------------------------------------------
-- 2. INSERT INTO dataset
------------------------------------------------------------
INSERT INTO
    dataset (
        id,
        uri,
        derived_card,
        derived_dataset,
        dataset_type,
        step,
        globs,
        languages,
        name,
        description,
        source,
        version,
        issued,
        modified,
        license
    )
VALUES (
        '18bf9811-7285-4841-9b56-3b59a0e74499',
        'file:///Users/T.Finizzi/repo/SFT-data-Forge/nfs/data-download/velvet_v1/allenai',
        'e79a8506-fe8e-456e-b5bb-ec3ee2d2a5bb',
        NULL,
        'benchmark',
        1,
        '{ai2_arc/ARC-Challenge/*,ai2_arc/ARC-Challenge/*.parquet,ai2_arc/ARC-Easy/*,ai2_arc/ARC-Easy/*.parquet}',
        '{en}',
        'allenai/ai2_arc',
        'A new dataset of 7,787 genuine grade-school level, multiple-choice science questions, assembled to encourage research in advanced question-answering. The dataset is partitioned into a Challenge Set and an Easy Set, where the former contains only questions answered incorrectly by both a retrieval-based algorithm and a word co-occurrence algorithm. We are also including a corpus of over 14 million science sentences relevant to the task, and an implementation of three neural baseline models for this dataset. We pose ARC as a challenge to the community.',
        'https://huggingface.co/datasets/allenai/ai2_arc',
        '1.0.0',
        '2026-01-01 00:00:00+00',
        '2026-01-01 00:00:00+00',
        'cc-by-sa-4.0'
    );

INSERT INTO
    dataset (
        id,
        uri,
        derived_card,
        derived_dataset,
        dataset_type,
        step,
        globs,
        languages,
        name,
        description,
        source,
        version,
        issued,
        modified,
        license
    )
VALUES (
        '18bf9811-7285-4841-9b56-3b59a0e74400',
        'file:///Users/T.Finizzi/repo/SFT-data-Forge/nfs/processed-data/velvet_v1/allenai',
        'e79a8506-fe8e-456e-b5bb-ec3ee2d2a5bb',
        '18bf9811-7285-4841-9b56-3b59a0e74499',
        'benchmark',
        2,
        '{ai2_arc/ARC-Challenge/en/*,ai2_arc/ARC-Challenge/en/*.gz}',
        '{en}',
        'processed__allenai/ai2_arc',
        'A new dataset of 7,787 genuine grade-school level, multiple-choice science questions, assembled to encourage research in advanced question-answering. The dataset is partitioned into a Challenge Set and an Easy Set, where the former contains only questions answered incorrectly by both a retrieval-based algorithm and a word co-occurrence algorithm. We are also including a corpus of over 14 million science sentences relevant to the task, and an implementation of three neural baseline models for this dataset. We pose ARC as a challenge to the community.',
        'https://huggingface.co/datasets/allenai/ai2_arc',
        '1.0.0',
        '2026-01-01 00:00:00+00',
        '2026-01-01 00:00:00+00',
        'cc-by-sa-4.0'
    );

INSERT INTO
    dataset (
        id,
        uri,
        derived_card,
        derived_dataset,
        dataset_type,
        step,
        globs,
        languages,
        name,
        description,
        source,
        version,
        issued,
        modified,
        license
    )
VALUES (
        '18bf9811-7285-4841-9b56-3b59a0e74420',
        'file:///Users/T.Finizzi/repo/SFT-data-Forge/nfs/mapped-data/velvet_v1/allenai',
        'e79a8506-fe8e-456e-b5bb-ec3ee2d2a5bb',
        '18bf9811-7285-4841-9b56-3b59a0e74400',
        'unknown',
        3,
        '{ai2_arc/ARC-Challenge/en/*,ai2_arc/ARC-Challenge/en/*.gz}',
        '{en}',
        'mapped__allenai/ai2_arc',
        'A new dataset of 7,787 genuine grade-school level, multiple-choice science questions, assembled to encourage research in advanced question-answering. The dataset is partitioned into a Challenge Set and an Easy Set, where the former contains only questions answered incorrectly by both a retrieval-based algorithm and a word co-occurrence algorithm. We are also including a corpus of over 14 million science sentences relevant to the task, and an implementation of three neural baseline models for this dataset. We pose ARC as a challenge to the community.',
        'https://huggingface.co/datasets/allenai/ai2_arc',
        '1.0.0',
        '2026-01-01 00:00:00+00',
        '2026-01-01 00:00:00+00',
        'cc-by-sa-4.0'
    );

------------------------------------------------------------
-- 3. INSERT INTO distribution
------------------------------------------------------------
INSERT INTO
    distribution (
        id,
        uri,
        tokenized_uri,
        dataset_id,
        step,
        glob,
        format,
        query,
        derived_from,
        src_schema,
        name,
        description,
        lang,
        split,
        materialized,
        tags,
        license,
        version,
        issued,
        modified
    )
VALUES (
        'bb13ddbd-e11b-462f-9417-35a4cb95b87a',
        'file:///Users/T.Finizzi/repo/SFT-data-Forge/nfs/data-download/velvet_v1/allenai/ai2_arc/ARC-Challenge',
        NULL,
        '18bf9811-7285-4841-9b56-3b59a0e74499',
        1,
        '*.parquet',
        'parquet',
        '{}',
        NULL,
        '{"type": "object", "$schema": "http://json-schema.org/schema#", "required": ["answerKey", "id", "label", "question", "text"], "properties": {"id": {"type": "string"}, "text": {"type": "array", "items": {"type": "string"}}, "label": {"type": "array", "items": {"type": "string"}}, "question": {"type": "string"}, "answerKey": {"type": "string"}}}',
        'ARC-Challenge',
        'This dataset contains multiple-choice questions related to scientific investigations. Each question presents a scenario and asks the user to select the best answer from a list of options. The example focuses on experimental design and data recording.',
        'en',
        'unknown',
        TRUE,
        '{multiple-choice,science,education,question-answering}',
        'cc-by-sa-4.0',
        '1.0.8',
        '2026-01-01 00:00:00+00',
        '2026-01-01 00:00:00+00'
    );

INSERT INTO
    distribution (
        id,
        uri,
        tokenized_uri,
        dataset_id,
        step,
        glob,
        format,
        query,
        derived_from,
        src_schema,
        name,
        description,
        lang,
        split,
        materialized,
        tags,
        license,
        version,
        issued,
        modified
    )
VALUES (
        '3df02834-896d-466c-a082-0274e9254b83',
        'file:///Users/T.Finizzi/repo/SFT-data-Forge/nfs/processed-data/velvet_v1/allenai/ai2_arc/ARC-Challenge/en',
        NULL,
        '18bf9811-7285-4841-9b56-3b59a0e74400',
        2,
        '*.jsonl.gz',
        'jsonl.gz',
        '{}',
        'bb13ddbd-e11b-462f-9417-35a4cb95b87a',
        '{}',
        'ARC-Challenge__en',
        'This dataset contains multiple-choice questions related to scientific investigations. Each question presents a scenario and asks the user to select the best answer from a list of options. The example focuses on experimental design and data recording. - EN version',
        'en',
        'unknown',
        't',
        '{multiple-choice,science,education,question-answering,lang:en}',
        'cc-by-sa-4.0',
        '1.0.8',
        '2026-01-01 00:00:00+00',
        '2026-01-01 00:00:00+00'
    );

INSERT INTO
    distribution (
        id,
        uri,
        tokenized_uri,
        dataset_id,
        glob,
        format,
        query,
        derived_from,
        src_schema,
        name,
        description,
        lang,
        split,
        materialized,
        tags,
        license,
        version,
        issued,
        modified,
        step
    )
VALUES (
        'a9a55ac3-e220-480d-a06e-cc4005960414',
        'file:///Users/T.Finizzi/repo/SFT-data-Forge/nfs/mapped-data/velvet_v1/allenai/ai2_arc/ARC-Challenge/en',
        NULL,
        '18bf9811-7285-4841-9b56-3b59a0e74420',
        '*.jsonl.gz',
        'jsonl.gz',
        NULL,
        '3df02834-896d-466c-a082-0274e9254b83',
        $${
        "type": "object",
        "$schema": "http://json-schema.org/draft-07/schema#",
        "required": [
            "messages",
            "_lang",
            "_dataset_path",
            "_subpath",
            "_filename",
            "_dataset_name",
            "_id_hash"
        ],
        "properties": {
            "_lang": {
                "type": "string",
                "default": "un",
                "maxLength": 2
            },
            "system": {
                "type": ["string", "null"]
            },
            "context": {
                "type": ["string", "null"]
            },
            "_id_hash": {
                "type": "string",
                "pattern": "^[a-fA-F0-9]{64}$"
            },
            "_subpath": {
                "type": "string",
                "minLength": 1
            },
            "messages": {
                "type": "array",
                "minItems": 1,
                "items": {
                    "type": "object",
                    "required": [
                        "content",
                        "role",
                        "think",
                        "functioncall",
                        "context"
                    ],
                    "properties": {
                        "role": {
                            "enum": ["USER", "ASSISTANT", "SYSTEM"],
                            "type": "string"
                        },
                        "think": {
                            "anyOf": [
                                {"type": "string", "minLength": 1},
                                {"type": "null"}
                            ]
                        },
                        "content": {
                            "anyOf": [
                                {"type": "string", "minLength": 1},
                                {"type": "null"}
                            ]
                        },
                        "context": {
                            "type": ["string", "null"]
                        },
                        "functioncall": {
                            "anyOf": [
                                {"type": "null"},
                                {
                                    "type": "object",
                                    "required": ["payload", "response"],
                                    "properties": {
                                        "payload": {
                                            "type": "string",
                                            "minLength": 1
                                        },
                                        "response": {
                                            "type": "string",
                                            "minLength": 1
                                        }
                                    },
                                    "additionalProperties": false
                                }
                            ]
                        }
                    },
                    "additionalProperties": false
                }
            },
            "_filename": {
                "type": "string",
                "minLength": 1
            },
            "_dataset_name": {
                "type": "string",
                "minLength": 1
            },
            "_dataset_path": {
                "type": "string",
                "minLength": 1
            }
        },
        "additionalProperties": false
    }$$,
        'mapped__ARC-Challenge/en',
        NULL,
        'en',
        'unknown',
        TRUE,
        '{}',
        'cc-by-sa-4.0',
        '1.0',
        '2026-01-28 00:00:00+01',
        '2026-01-28 00:00:00+01',
        3
    );

-- UDF

INSERT INTO
    udf (
        id,
        name,
        description,
        function_definition,
        example_params,
        issued,
        modified
    )
VALUES (
        '53b81649-c5a6-4529-b8f1-87f345bd8615',
        'ARC_simple_chat',
        'concat Q to multiple A',
        'def ARC_simple_chat(func_name: str, param_1: str | list[str], param_2: list[str]) -> str:
    res=""

    if isinstance(param_1, list):
        for t in param_1:
            if not isinstance(t, str):
                res += str(t)
            else:
                res += t
    else:
        res = str(param_1)

    if isinstance(param_2[0], list):
        res += "\n" + "A: " + str(param_2[0][0])
        if len(param_2[0]) > 1:
            res += "\n" + "B: " + str(param_2[0][1])
        if len(param_2[0]) > 2:
            res += "\n" + "C: " + str(param_2[0][2])
        if len(param_2[0]) > 3:
            res += "\n" + "D: " + str(param_2[0][3])
    else:
        res += "\n" + "A: " + str(param_2[0])
        if len(param_2) > 1:
            res += "\n" + "B: " + str(param_2[1])
        if len(param_2) > 2:
            res += "\n" + "C: " + str(param_2[2])
        if len(param_2) > 3:
            res += "\n" + "D: " + str(param_2[3])

    return res',
        ARRAY[
            '{\"type\": \"str\", \"value\": \"ARC_simple_chat\"}',
            '{\"type\": \"str\", \"value\": \"capitale Italia?\"}',
            '{\"type\": \"list\", \"value\": [\"MI\", \"RM\", \"VE\", \"NA\"]}'
        ],
        '2026-01-28 11:27:28.972377+01',
        '2026-01-28 11:27:28.972379+01'
    );

-- mapping

INSERT INTO
    mapping (
        id,
        distribution_id,
        schema_template_id,
        mapping,
        version,
        issued,
        modified
    )
VALUES (
        '3df02834-896d-466c-a082-0274e9254b00',
        '3df02834-896d-466c-a082-0274e9254b83',
        '3df02834-896d-466c-a082-0274e9254b95',
        $${
        "_lang": ["_lang"],
        "system": ["set_fixed_value", null],
        "context": ["set_fixed_value", null],
        "_id_hash": ["_id_hash"],
        "_subpath": ["_subpath"],
        "_filename": ["_filename"],
        "_dataset_name": ["_dataset_name"],
        "_dataset_path": ["_dataset_path"],
        "messages[0].role": ["set_fixed_value", "USER"],
        "messages[1].role": ["set_fixed_value", "ASSISTANT"],
        "messages[0].think": ["set_fixed_value", null],
        "messages[1].think": ["set_fixed_value", null],
        "messages[0].content": ["ARC_simple_chat", "question", "text"],
        "messages[0].context": ["set_fixed_value", null],
        "messages[1].content": ["answerKey"],
        "messages[1].context": ["set_fixed_value", null],
        "messages[0].functioncall": ["set_fixed_value", null],
        "messages[1].functioncall": ["set_fixed_value", null]
    }$$,
        '1.0.1',
        '2026-01-28 11:25:57.272367+01',
        '2026-01-28 11:27:44.482267+01'
    );


