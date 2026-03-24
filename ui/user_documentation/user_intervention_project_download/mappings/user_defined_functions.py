from typing import Any, List

from .transform_functions import *


def glaive_rag_v1_content_pipe(func_name: str, text: Any, prefix: str, start_tag_regex: str, end_tag_regex: str, concat_string: str) -> List[Any]:
    """
    Esegue una serie di funzioni in sequenza su un dataset che ha bisogno di
    pulizia e estrazione di operazioni specifiche."""
    if not text or not isinstance(text, str) or not prefix or not start_tag_regex or not end_tag_regex:
        return []

    # 1. Rimuove il prefisso specificato
    text = remove_prefix("remove_prefix", prefix, text)[0]
    # 2. Estrae il contenuto dei tag specificati
    text = extract_and_aggregate_tag_object_content_id("extract_and_aggregate_tag_object_content_id", text, start_tag_regex, end_tag_regex,concat_string)[0]
    return [text]

def ARC_simple_chat(func_name: str, param_1: str | list[str], param_2: list[str]) -> str:
    res=''
    if isinstance(param_1, list):
        for t in param_1:
            if not isinstance(t, str):
                res += str(t)
            else: 
                res += t
    else:
        res = str(param_1)

    if isinstance(param_2[0], list):
        res += "\n" +"A: " + str(param_2[0][0])
        if len(param_2[0]) > 1:
            res += "\n" +"B: " + str(param_2[0][1])
        if len(param_2[0]) > 2:
            res += "\n" +"C: " + str(param_2[0][2])
        if len(param_2[0]) > 3:
            res += "\n" +"D: " + str(param_2[0][3])
    else:
        res += "\n" +"A: " + str(param_2[0])
        if len(param_2) > 1:
            res += "\n" +"B: " + str(param_2[1])
        if len(param_2) > 2:
            res += "\n" +"C: " + str(param_2[2])
        if len(param_2) > 3:
            res += "\n" +"D: " + str(param_2[3])
    return res

