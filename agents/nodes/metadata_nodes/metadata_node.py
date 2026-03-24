from agents.states.dataset_metadata_state import State
import json
from json_repair import repair_json
from langchain_core.messages import HumanMessage, AIMessage, BaseMessage
from langgraph.types import Command
from typing import Literal

class MetadataNode:
    def __init__(self, llm, prompt: str, feedback_prompt: str = None):
        self.llm = llm
        self.prompt = prompt
        self.feedback_prompt = feedback_prompt

    def _ensure_lc_messages(self, chat_history: list) -> list[BaseMessage]:
        """
        Converte dizionari raw in HumanMessage/AIMessage.
        """
        lc_msgs: list[BaseMessage] = []
        for msg in chat_history:
            if isinstance(msg, dict):
                if msg.get("role") == "user":
                    lc_msgs.append(HumanMessage(content=msg["content"]))
                elif msg.get("role") == "assistant":
                    lc_msgs.append(AIMessage(content=msg["content"]))
            elif isinstance(msg, (HumanMessage, AIMessage)):
                lc_msgs.append(msg)
            else:
                raise ValueError(f"Tipo di messaggio non supportato: {type(msg)}")
        return lc_msgs

    def _extract_json(self, json_text: str) -> dict:
        """
        Ripara e deserializza l'output JSON generato da LLM.
        Restituisce un dict oppure un'eccezione in caso di errore.
        """
        try:
            repaired_text = repair_json(json_text)
            parsed_json = json.loads(repaired_text)
            if not isinstance(parsed_json, dict):
                raise ValueError("Parsed JSON is not a dict")
            return parsed_json
        except Exception as e:
            return {"error": str(e)}

    def __call__(self, state: State) -> Command[Literal["human_node"]]:
        """
        Genera i Metadati JSON a partire dai samples
        """
        print("Generating metadata with LLM...")
            
        samples_json = ""
        for item in state.samples:
            if not isinstance(item, dict):
                raise ValueError("All samples must be dictionaries")
            samples_json += json.dumps(item, indent=2) + "\n---\n"
        
        if not state.feedback:
            msg_content = self.prompt.format(samples=samples_json)
        else:
            msg_content = self.prompt.format(samples=samples_json,)
            msg_content += self.feedback_prompt.format(feedback=state.feedback)
            print("message to LLM with feedback: ", msg_content)

        state.chat_history.append(HumanMessage(content=msg_content))
        lc_chat_history = self._ensure_lc_messages(state.chat_history)
        response = self.llm.invoke(lc_chat_history)
            
        generated_metadata = self._extract_json(response.content)

        assistant_msg = AIMessage(
                content=json.dumps(generated_metadata, indent=2)
        )

        return Command(
                goto="human_node",
                update={
                    "chat_history": state.chat_history + [assistant_msg],
                    "accept_metadata_generation": None,
                    "feedback": None,
                })