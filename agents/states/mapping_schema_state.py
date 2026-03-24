from typing import List, Dict, Any, Literal, Optional
from typing_extensions import Annotated
from pydantic import Field, BaseModel
from langchain_core.messages import BaseMessage


class State(BaseModel):
    samples: List[Dict[str, Any]] = Field(default=[], description="Esempi di dati grezzi")
    mapped_samples: List[Optional[Dict[str, Any]]] = Field(default=[], description="Esempi di dati trasformati")
    metadata: Optional[Dict[str, Any]] = Field(default=None, description="Metadati associati ai dati")

    src_schema: Optional[Dict[str, Any]] = Field(default=None, description="Schema dei dati")
    dst_schema: Optional[Dict[str, Any]] = Field(default=None, description="Schema di destinazione per il mapping")

    chat_history: Annotated[List[BaseMessage], "Conversation"] = Field(default=[], description="Storico della conversazione con l'LLM")
    feedback: Optional[str] = Field(default=None, description="Feedback dell'utente sulla generazione dello schema")

    mapping: Optional[Dict[str, List[Any]]] = Field(default=None, description="Mapping generato tra src_schema e dst_schema")
    accept_mapping_generation: Optional[Literal["continue", "break", "restart", "manual"]] = Field(default=None, description="Decisione dell'utente sulla generazione dello schema")

    valid: bool = Field(default=False, description="Indica se il mapping è valido rispetto allo schema di destinazione")

    output_path: Optional[str] = Field(default='', description="Percorso di output per il salvataggio dello schema")
    error_messages: List[str] = Field(default=[], description="Lista di errori di validazione")