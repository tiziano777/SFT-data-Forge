import sys
sys.path.append("..")
from datatrove.executor import LocalPipelineExecutor
from reader.unified_reader import UnifiedReader
from stats.low_level_stats import DocStats
from extractor.map import MapperExtractor
from stats.chat_template_stats import ChatTemplateStats
from writer.writer import CustomJsonlWriter

from typing import Dict, List, Any
MappingSpec = Dict[str, List[Any]]

import os


PROCESSED_DATA_DIR= os.getenv("PROCESSED_DATA_DIR")
MAPPED_DATA_DIR= os.getenv("MAPPED_DATA_DIR")
STATS_DATA_DIR= os.getenv("STATS_DATA_DIR")
LOW_LEVEL_STATS_EXTENSION= os.getenv("LOW_LEVEL_STATS_EXTENSION")

def run_mapping_pipeline():

    # PATHS:
    stats_path = STATS_DATA_DIR + "/velvet_v1/allenai/ARC-Challenge/en/"
    processed_dataset_path = PROCESSED_DATA_DIR + "/velvet_v1/allenai/"
    processed_distribution_path = processed_dataset_path + "ARC-Challenge/en/"
    mapped_dataset_path = MAPPED_DATA_DIR + "/velvet_v1/allenai/"
    low_level_stats_path= stats_path + LOW_LEVEL_STATS_EXTENSION

    input_path= processed_dataset_path
    output_path= mapped_dataset_path
    src_schema= {"type": "object", "$schema": "http://json-schema.org/schema#", "required": [ "_dataset_name", "_dataset_path", "_filename", "_id_hash", "_lang", "_subpath", "answerKey", "id", "label", "question", "text"], "properties": {"id": {"type": "string"}, "text": {"type": "array", "items": {"type": "string"}}, "_lang": {"type": "string"}, "label": {"type": "array", "items": {"type": "string"}}, "_id_hash": {"type": "string"}, "_subpath": {"type": "string"}, "question": {"type": "string"}, "_filename": {"type": "string"}, "answerKey": {"type": "string"}, "_dataset_name": {"type": "string"}, "_dataset_path": {"type": "string"}}}
    dst_schema = {
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
            "template": {
            "type": "string",
            "enum": [
                "simple_chat",
                "context_chat",
                "simple_chat_think",
                "context_chat_think",
                "fc_chat_st",
                "fc_chat_dy"
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
                        "additionalProperties": False
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
                "additionalProperties": False
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
            },
            "content_length": {
                "type": "integer"
            }
        },
        "required": [
            "template",
            "messages",
            "_lang",
            "_dataset_path",
            "_subpath",
            "_filename",
            "_dataset_name",
            "_id_hash",
        ],
        "additionalProperties": False
        }
    mapping= {"_lang": ["_lang"], "system": ["set_fixed_value", None], "context": ["set_fixed_value", None], "_id_hash": ["_id_hash"], "_subpath": ["_subpath"], "template": ["set_fixed_value", "simple_chat"], "_filename": ["_filename"], "_dataset_name": ["_dataset_name"], "_dataset_path": ["_dataset_path"], "messages[0].role": ["set_fixed_value", "USER"], "messages[1].role": ["set_fixed_value", "ASSISTANT"], "messages[0].think": ["set_fixed_value", None], "messages[1].think": ["set_fixed_value", None], "messages[0].content": ["ARC_simple_chat", "question", "text"], "messages[0].context": ["set_fixed_value", None], "messages[1].content": ["answerKey"], "messages[1].context": ["set_fixed_value", None], "messages[0].functioncall": ["set_fixed_value", None], "messages[1].functioncall": ["set_fixed_value", None]}

    print("🚀 Starting Mapping Pipeline")
    print(f"Input path: {input_path}")
    print(f"Output path: {output_path}")
    print(f"Low level stats path: {low_level_stats_path}")


    reader= UnifiedReader(
        data_folder=processed_distribution_path,
        glob_pattern="*.jsonl.gz", # current glob of distribution processed data
        recursive=True,
        text_key="text",
        id_key="id",
        default_metadata={} 
    )
    low_level_stats = DocStats(
        output_folder=low_level_stats_path,
    )

    mapper= MapperExtractor(
        mapping_spec=mapping,
        dst_schema=dst_schema,
        src_schema=src_schema
    )



    writer= CustomJsonlWriter(
        base_input_path=input_path,
        base_output_path=output_path,
    )

    pipeline= [reader,low_level_stats,mapper,writer]
    
    pipe = LocalPipelineExecutor(
        pipeline=pipeline,
        tasks=1,
        logging_dir=None,
    )
    pipe.run()

if __name__ == "__main__":
    run_mapping_pipeline()
