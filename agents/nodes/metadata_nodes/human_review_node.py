from langgraph.types import interrupt, Command
from typing import Literal
from agents.states.dataset_metadata_state import State
import json

class HumanReviewNode:
    def __init__(self):
        pass

    def __call__(self,state: State) -> Command[Literal["llm_node"]]:
        """
        Human interrupt for Metadata review and manual correction, explicit approval/rejection.
        """
        print("Human review of the generated Metadata...")
        

        decision = interrupt({
            "assistant_output": state.chat_history[-1].content,
            "chat_history": state.chat_history,
            "instructions": "Decidi se accettare il Metadata (break), fornire feedback (continue), resettare la chat (restart) oppure modificarla manualmente."
        })
        
        # decision = {"action":"break"}
        # oppure {"action":"continue","feedback":"suggested corrections"}
        # oppure {"action":"restart"}
        # oppure {"action":"manual","feedback":"final JSON schema"}
        action = decision["action"]

        if action == "break":
            print("User accept the generated Metadata.")
            return Command(
                goto="writer_node",
                update={
                    "accept_metadata_generation":"break",
                    "metadata": json.loads(state.chat_history[-1].content)
                }
            )
        
        elif action == "continue":
            print("User provides feedback for metadata improvement.")
            feedback_msg = str(decision["feedback"])
            return Command(
                goto="llm_node",
                update={
                    "accept_metadata_generation": "continue",
                    "feedback": feedback_msg
                }
            )

        elif action == "restart":
            print("User requests to restart the metadata generation process.")
            return Command(
                goto="llm_node",
                update={
                    "accept_metadata_generation": "restart",
                    "chat_history": [],
                    "metadata": None,
                    "feedback": None
                }
            )
        
        elif action == "manual":
            print("User provides a manual metadata.")
            manual_metadata = json.loads(decision["feedback"])
            try:
                
                return Command(
                    goto="writer_node",
                    update={
                        "accept_metadata_generation": "manual",
                        "metadata": manual_metadata
                    }
                )
            except json.JSONDecodeError:
                raise ValueError("Invalid JSON metadata provided by the user.")
            
        else:
            raise ValueError("Invalid human decision")