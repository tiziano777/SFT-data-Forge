from langgraph.types import interrupt, Command
from typing import Literal
from agents.states.mapping_schema_state import State
import json
from langchain_core.messages import AIMessage

class HumanReviewNode:
    def __init__(self):
        pass

    def __call__(self, state: State) -> Command[Literal["llm_node","validation_node"]]:
        """
        Human interrupt for mapping review and manual correction.
        """
        print("Human review of the generated mapping...")
        print(f"Stato corrente - feedback: {state.feedback}")
        print(f"Stato corrente - mapping: {state.mapping is not None}")
        
        # 🔥 DEBUG: Mostra cosa c'è nel chat_history
        if state.chat_history:
            print(f"Ultimo messaggio LLM: {state.chat_history[-1].content[:100]}...")
        
        decision = interrupt({
            "assistant_output": state.chat_history[-1].content if state.chat_history else "{}",
            "chat_history": state.chat_history,
            "instructions": "Decidi se accettare il mapping (break), fornire feedback (continue), resettare la chat (restart) oppure modificarla manualmente."
        })
        
        action = decision["action"]
        print(f"User action: {action}")

        if action == "break":
            print("User accept the generated mapping.")
            # 🔥 USA IL MAPPING CORRENTE dallo stato, non dal chat_history
            current_mapping = state.mapping if state.mapping else json.loads(state.chat_history[-1].content)
            return Command(
                goto="validation_node",
                update={
                    "accept_mapping_generation": "break",
                    "mapping": current_mapping
                }
            )
        
        elif action == "continue":
            print(f"User provides feedback for mapping improvement: {decision['feedback']}")
            return Command(
                goto="llm_node",
                update={
                    "accept_mapping_generation": "continue",
                    "feedback": str(decision["feedback"]),
                    "mapping": None  # 🔥 Reset per forzare rigenerazione
                }
            )

        elif action == "restart":
            print("User requests to restart the mapping generation process.")
            return Command(
                goto="llm_node",
                update={
                    "accept_mapping_generation": "restart",
                    "chat_history": [],
                    "mapping": None,
                    "feedback": None
                }
            )
        
        elif action == "manual":
            print("User provides a manual mapping.")
            try:
                # 🔥 CORREZIONE CRITICA: Gestione robusta del mapping manuale
                manual_feedback = decision["feedback"]
                print(f"Raw manual feedback type: {type(manual_feedback)}, content: {manual_feedback[:200] if isinstance(manual_feedback, str) else manual_feedback}")
                
                # Se è una stringa, parsala come JSON
                if isinstance(manual_feedback, str):
                    manual_mapping = json.loads(manual_feedback)
                else:
                    # Se è già un dict, usalo direttamente
                    manual_mapping = manual_feedback
                
                print(f"✅ Manual mapping parsed successfully: {manual_mapping.get('_lang')}")
                
                # 🔥 AGGIORNAMENTO CRITICO: Sovrascrivi COMPLETAMENTE lo stato
                return Command(
                    goto="validation_node",
                    update={
                        "accept_mapping_generation": "manual",
                        "mapping": manual_mapping,  # 🔥 Questo è il mapping MODIFICATO
                        "chat_history": [AIMessage(content=json.dumps(manual_mapping, indent=2))]  # 🔥 Reset chat con nuovo mapping
                    }
                )
                
            except json.JSONDecodeError as e:
                print(f"❌ JSON decode error in manual mapping: {e}")
                # Fallback: ritorna al LLM
                return Command(
                    goto="llm_node",
                    update={
                        "accept_mapping_generation": "continue",
                        "feedback": f"Errore nel JSON manuale: {e}. Per favore correggi e riprova."
                    }
                )
            except Exception as e:
                print(f"❌ Error in manual mapping processing: {e}")
                raise ValueError(f"Invalid manual mapping: {e}")
            
        else:
            raise ValueError("Invalid human decision")