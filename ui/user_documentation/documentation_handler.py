# ui/user_documentation/documentation_handler.py
from ui.user_documentation.doc_engine.doc_engine import scan_docs, render_page 

def documentation(st):
    docs = scan_docs()
    render_page(docs)



