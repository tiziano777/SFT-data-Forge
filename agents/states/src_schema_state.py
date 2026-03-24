from typing import List, Dict, Any, Literal, Optional
from typing_extensions import Annotated
from pydantic import Field, BaseModel
from langchain_core.messages import BaseMessage


class State(BaseModel):
    samples: List[Dict[str, Any]] = Field(default=[], description="Esempi di dati grezzi")
    deterministic: bool = Field(default=False, description="Flag per la generazione deterministica")

    chat_history: Annotated[List[BaseMessage], "Conversation"] = Field(default=[], description="Storico della conversazione con l'LLM")

    accept_schema_generation: Optional[Literal["continue", "break", "restart", "manual"]] = Field(default=None, description="Decisione dell'utente sulla generazione dello schema")
    feedback: Optional[str] = Field(default=None, description="Feedback dell'utente sulla generazione dello schema")

    valid: Optional[bool] = Field(default=None, description="Flag di validità generale dello stato")

    generated_schema: Optional[Dict[str, Any]] = Field(default=None, description="Schema dei dati")

    output_path: Optional[str] = Field(default='', description="Percorso di output per il salvataggio dello schema")

    error_messages: List[str] = Field(default=[], description="Lista di errori di validazione")

