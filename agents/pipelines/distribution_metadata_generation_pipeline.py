import traceback

from langgraph.graph import StateGraph, START
from langgraph.checkpoint.memory import InMemorySaver

from agents.states.dataset_metadata_state import State

from agents.nodes.metadata_nodes.metadata_node import MetadataNode
from agents.nodes.metadata_nodes.human_review_node import HumanReviewNode
from agents.nodes.metadata_nodes.metadata_writer_node import MetadataWriter


def create_pipeline(llm_node: MetadataNode,
                    human_node: HumanReviewNode,
                    writer_node: MetadataWriter) -> StateGraph:

    graph = StateGraph(State)

    graph.add_node("llm_node", llm_node)
    graph.add_node("human_node", human_node)
    graph.add_node("writer_node", writer_node)

    graph.add_edge(START,"llm_node")

    # loop di feedback
    #graph.add_edge("llm_node", "human_node")
    #graph.add_edge("human_node", "llm_node")  

    # se human conferma, vai a writer
    #graph.add_edge("human_node", "writer_node")
    #graph.add_edge("writer_node", END)           
    
    checkpointer = InMemorySaver()
    graph = graph.compile(checkpointer=checkpointer)

    
    '''
    try:
        graphImage = graph.get_graph().draw_mermaid_png()
        with open("docs/images/gemini_api_llm_metadata_schema_pipeline.png", "wb") as f:
            f.write(graphImage)
        print("Salvata immagine del grafo in gemini_api_llm_metadata_schema_pipeline.png")
    except Exception as e:
        print(f"Errore durante la generazione del grafo: {e}")
    '''
    return graph

