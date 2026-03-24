import traceback

from langgraph.graph import StateGraph, START
from langgraph.checkpoint.memory import InMemorySaver

from agents.states.src_schema_state import State

from agents.nodes.src_schema_nodes.schema_node import SchemaNode
from agents.nodes.src_schema_nodes.human_review_node import HumanReviewNode
from agents.nodes.src_schema_nodes.validation_node import ValidationNode
from agents.nodes.src_schema_nodes.schema_writer_node import SchemaWriter


def create_pipeline(llm_node: SchemaNode,
                    human_node: HumanReviewNode,
                    validation_node: ValidationNode,
                    writer_node: SchemaWriter) -> StateGraph:

    graph = StateGraph(State)

    graph.add_node("llm_node", llm_node)
    graph.add_node("human_node", human_node)
    graph.add_node("validation_node", validation_node)
    graph.add_node("writer_node", writer_node)

    graph.add_edge(START,"llm_node")

    # loop di feedback
    #graph.add_edge("llm_node", "human_node")
    #graph.add_edge("human_node", "llm_node")  
    # After human review, go to validation
    #graph.add_edge("human_node", "validation_node")
    # se validazione fallisce
    #graph.add_edge("validation_node", "llm_node")    
    # se validazione ok
    #graph.add_edge("validation_node", END)           
    
    checkpointer = InMemorySaver()
    graph = graph.compile(checkpointer=checkpointer)

    
    '''
    try:
        graphImage = graph.get_graph().draw_mermaid_png()
        with open("docs/images/gemini_api_llm_src_schema_pipeline.png", "wb") as f:
            f.write(graphImage)
        print("Salvata immagine del grafo in gemini_api_llm_src_schema_pipeline.png")
    except Exception as e:
        print(f"Errore durante la generazione del grafo: {e}")
        print(traceback.format_exc())
    '''

    return graph


