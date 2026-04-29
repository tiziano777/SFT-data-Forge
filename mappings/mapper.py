import re
import inspect
import jsonschema
from typing import Any, Dict, List, Callable, Tuple, Optional

# Importazioni per trasformazioni e DB
from . import transform_functions 
from db.impl.postgres.loader.postgres_db_loader import get_db_manager
from data_class.repository.table.udf_repository import UdfRepository

import logging
logger = logging.getLogger(__name__)
#logger.setLevel(logging.DEBUG)  # Imposta il livello di log su DEBUG per vedere i messaggi di debug

# Definizione tipi
MappingSpec = Dict[str, List[Any]]
JsonDocument = Dict[str, Any]

def create_function_wrapper(func):
    """Crea un wrapper che SEMPRE passa il nome della funzione come primo argomento."""
    def wrapper(op_spec, *args):
        return func(op_spec, *args)
    return wrapper

# --- POOL STATICO (Caricato una sola volta all'import) ---
# Contiene le funzioni predefinite nel file transform_functions.py
STATIC_FUNCTION_POOL = {}
for name, func in inspect.getmembers(transform_functions, inspect.isfunction):
    STATIC_FUNCTION_POOL[name] = create_function_wrapper(func)

class Mapper:
    """
    Classe per mappare i valori da un documento JSON sorgente a un documento
    JSON destinazione con validazione e funzioni di trasformazione modulari.
    """

    _NAVIGATION_PATTERN = re.compile(r'(\w+)(\[\d+\]|\[\])?')

    def __init__(self, mapping_spec: MappingSpec, dst_schema: Dict, src_schema: Dict):
        
        if src_schema is None:
            raise ValueError("Lo schema sorgente è obbligatorio per determinare i path validi")
            
        self.mapping_spec = mapping_spec
        self.src_schema = src_schema
        
        # 1. Inizializza il registro con le funzioni statiche di base
        self._function_registry: Dict[str, Callable] = STATIC_FUNCTION_POOL.copy() 
        
        # 2. CARICAMENTO DINAMICO UDF DAL DATABASE
        # Carichiamo le funzioni qui dentro per garantire che siano sempre le ultime versioni salvate
        try:
            db_manager = get_db_manager()
            udf_repository = UdfRepository(db_manager)
            udfs_from_db = udf_repository.get_all()
            
            for udf in udfs_from_db:
                try:
                    namespace = {}
                    # Eseguiamo la definizione della funzione nel namespace locale
                    exec(udf.function_definition, namespace)
                    
                    # Recuperiamo l'oggetto funzione cercandolo per nome
                    if udf.name in namespace:
                        udf_func = namespace[udf.name]
                        # Registriamo nel registro dell'istanza corrente col wrapper
                        self._function_registry[udf.name] = create_function_wrapper(udf_func)
                    else:
                        # Fallback se il nome nel codice non coincide col nome nel DB
                        callables = [f for f in namespace.values() if callable(f)]
                        if callables:
                            self._function_registry[udf.name] = create_function_wrapper(callables[0])
                except Exception as e:
                    print(f"Errore caricamento UDF dinamica '{udf.name}': {e}")
        except Exception as e:
            print(f"Errore connessione database durante init Mapper: {e}")

        # 3. Inizializzazione validatori e path
        self._current_src_doc = None 
        self.src_validator = jsonschema.Draft7Validator(src_schema)
        self.dst_validator = jsonschema.Draft7Validator(dst_schema)

        self._valid_source_paths = self._extract_valid_paths_from_schema(src_schema)
        self._field_mapping = self._analyze_schema_structure(src_schema)
        self.errors: List[str] = []

    def _analyze_schema_structure(self, schema: Dict) -> Dict[str, str]:
        """
        Analizza lo schema per individuare campi che sono stati appiattiti
        e crea un mapping dai nomi appiattiti ai path completi.
        """
        field_mapping = {}
        valid_paths_set = set(self._valid_source_paths)
        
        #print("DEBUG: Analyzing schema structure for flattened fields...")
        
        # Cerca campi che potrebbero essere appiattiti
        for path in valid_paths_set:
            # Se il path non ha punti (campo root) ma potrebbe essere nidificato
            if '.' not in path and not re.search(r'\[\d+\]', path):
                # Prova a trovare dove potrebbe essere realmente questo campo
                possible_parents = self._find_possible_parents(path, valid_paths_set)
                if possible_parents:
                    # Prendi il parent più probabile (il più corto)
                    best_parent = min(possible_parents, key=len)
                    field_mapping[path] = f"{best_parent}.{path}"
                    #print(f"DEBUG: Mapped flattened field '{path}' -> '{field_mapping[path]}'")
        
        return field_mapping

    def _find_possible_parents(self, field: str, valid_paths: set) -> List[str]:
        """
        Trova possibili genitori per un campo che potrebbe essere appiattito.
        """
        parents = []
        
        # Cerca path che contengono questo campo come parte finale
        for path in valid_paths:
            if path.endswith(f'.{field}') or path.endswith(f'.{field}[]'):
                parent = path.rsplit('.', 1)[0]
                if parent not in parents:
                    parents.append(parent)
        
        return parents

    def _extract_valid_paths_from_schema(self, schema: Dict, current_path: str = "") -> List[str]:
        """
        Estrae tutti i path validi dallo schema JSON sorgente.
        Gli indici numerici specifici (es. path[1],path[2],...) non vengono pre-generati:
        _is_source_path valida qualsiasi path[N] a runtime se path[] è presente.
        """
        valid_paths = []
        
        def extract_paths(obj: Any, path: str):
            if isinstance(obj, dict):
                if 'properties' in obj:
                    for key, value in obj['properties'].items():
                        new_path = f"{path}.{key}" if path else key
                        valid_paths.append(new_path)
                        extract_paths(value, new_path)
                
                elif 'items' in obj:
                    array_path = path + '[]'
                    valid_paths.append(array_path)
                    if isinstance(obj['items'], dict):
                        extract_paths(obj['items'], array_path)
                
                elif 'type' in obj:
                    if obj['type'] == 'array':
                        valid_paths.append(path + '[]')
                
                for key in ['anyOf', 'oneOf', 'allOf']:
                    if key in obj:
                        for item in obj[key]:
                            extract_paths(item, path)
            
            elif isinstance(obj, list):
                for item in obj:
                    extract_paths(item, path)
        
        extract_paths(schema, "")
        return list(set([p for p in valid_paths if p]))

    def _resolve_flattened_field(self, field: str) -> Optional[str]:
        """
        Risolve un campo che potrebbe essere appiattito nel suo path completo.
        Restituisce None se non viene trovato alcun mapping.
        """
        # Controlla se abbiamo un mapping esplicito
        if field in self._field_mapping:
            mapped_path = self._field_mapping[field]
            #print(f"DEBUG _resolve_flattened_field: '{field}' -> '{mapped_path}'")
            return mapped_path
        
        # Se non c'è mapping, prova a indovinare basandosi sui path validi
        possible_paths = [p for p in self._valid_source_paths if p.endswith(f'.{field}') or p == field]
        
        if len(possible_paths) == 1:
            # Un solo path possibile, usalo
            return possible_paths[0]
        elif len(possible_paths) > 1:
            # Più path possibili, scegli il più corto
            best_path = min(possible_paths, key=len)
            #print(f"DEBUG _resolve_flattened_field: '{field}' -> '{best_path}' (multiple options: {possible_paths})")
            return best_path
        
        return None

    def _get_values_from_path(self, doc: JsonDocument, path: str) -> List[Any]:
        """Estrae valori da un documento JSON usando dot notation con supporto per campi appiattiti."""
        if not path or not doc:
            return []

        try:
            direct_result = self._extract_values_direct_path(doc, path)
            if direct_result:
                #print(f"DEBUG _get_values_from_path: '{path}' -> DIRECT {direct_result}")
                return direct_result
            
            # Se il path diretto non funziona, prova a risolvere come campo appiattito
            resolved_path = self._resolve_flattened_field(path)
            if resolved_path and resolved_path != path:
                resolved_result = self._extract_values_direct_path(doc, resolved_path)
                #print(f"DEBUG _get_values_from_path: '{path}' -> RESOLVED '{resolved_path}' -> {resolved_result}")
                return resolved_result
            
            # Questo indica uno schema appiattito che non corrisponde alla struttura reale
            # Proviamo una ricerca esplorativa nel documento
            exploratory_result = self._exploratory_search(doc, path)
            if exploratory_result:
                #print(f"DEBUG _get_values_from_path: '{path}' -> EXPLORATORY {exploratory_result}")
                return exploratory_result
            
            #print(f"DEBUG _get_values_from_path: '{path}' -> NO VALUES FOUND (schema mismatch)")
            return []
            
        except Exception as e:
            #print(f"Errore durante l'estrazione del path '{path}': {e}")
            return []

    def _exploratory_search(self, doc: JsonDocument, field: str) -> List[Any]:
        """
        Cerca esplorativamente un campo nel documento quando c'è un mismatch schema-dati.
        Gestisce correttamente i tipi di dato composti (liste, array).
        """
        #print(f"DEBUG _exploratory_search: looking for '{field}' in document structure")
        
        # Cerca il campo a qualsiasi livello di nidificazione
        def find_in_nested(obj, target_field, current_path=""):
            results = []
            
            if isinstance(obj, dict):
                # Controlla se il campo è direttamente presente
                if target_field in obj:
                    value = obj[target_field]
                    if isinstance(value, list):
                        for i, item in enumerate(value):
                            results.append((current_path + f".{target_field}[{i}]" if current_path else f"{target_field}[{i}]", item))
                    else:
                        results.append((current_path + "." + target_field if current_path else target_field, value))
                
                # Cerca ricorsivamente in tutti i sotto-oggetti
                for key, value in obj.items():
                    new_path = current_path + "." + key if current_path else key
                    if isinstance(value, (dict, list)):
                        results.extend(find_in_nested(value, target_field, new_path))
            
            elif isinstance(obj, list):
                for i, item in enumerate(obj):
                    if isinstance(item, (dict, list)):
                        results.extend(find_in_nested(item, target_field, current_path + f"[{i}]"))
            
            return results
        
        found_items = find_in_nested(doc, field)
        
        if found_items:
            all_values = [value for path, value in found_items]
            #print(f"DEBUG _exploratory_search: found '{field}' at {len(found_items)} locations: {[path for path, _ in found_items]}")
            #print(f"DEBUG _exploratory_search: returning ALL values: {all_values}")
            return all_values
        
        return []

    def _extract_values_direct_path(self, doc: JsonDocument, path: str) -> List[Any]:
        """Estrae valori usando il path specificato senza ulteriori risoluzioni."""
        segments = path.split('.')
        current_data = [doc]
        
        for segment in segments:
            next_data = []
            match = self._NAVIGATION_PATTERN.match(segment)
            if not match:
                #print(f"DEBUG: Segmento non valido: {segment}")
                return []

            key, array_spec = match.groups()

            for item in current_data:
                if item is None:
                    continue
                    
                if isinstance(item, dict) and key in item:
                    value = item[key]
                else:
                    continue

                if not array_spec: 
                    next_data.append(value)
                elif array_spec == '[]':
                    if isinstance(value, list): 
                        next_data.extend(value)
                    else:
                        # Se non è una lista ma c'è [], trattalo come lista con un elemento
                        next_data.append(value)
                elif re.match(r'\[\d+\]', array_spec):
                    index = int(array_spec.strip('[]'))
                    if isinstance(value, list) and 0 <= index < len(value): 
                        next_data.append(value[index])
                    elif index == 0 and not isinstance(value, list):
                        # Se c'è [0] ma il valore non è una lista, restituisci il valore stesso
                        next_data.append(value)

            current_data = next_data
            
        return current_data

    def validate_source(self, sample: JsonDocument) -> bool:
        """Esegue la validazione sorgente e aggiunge gli errori a self.errors."""
        is_valid = True
        for error in sorted(self.src_validator.iter_errors(sample), key=str):
            self.errors.append(f"Validazione Schema Sorgente Fallita: {error.message} in {list(error.path)}")
            is_valid = False
        return is_valid
    
    def validate_destination(self, sample: JsonDocument) -> bool:
        """Esegue la validazione destinazione e aggiunge gli errori a self.errors."""
        is_valid = True
        for error in sorted(self.dst_validator.iter_errors(sample), key=str):
            self.errors.append(f"Validazione Schema Destinazione Fallita: {error.message} in {list(error.path)}")
            is_valid = False
        return is_valid

    def _get_parent_container(self, doc: JsonDocument, path: str) -> Dict[str, Any]:
        """Ottiene il contenitore padre per il path specificato."""
        if not path:
            return doc
            
        segments = path.split('.')
        current = doc
        
        for segment in segments[:-1]:
            match = self._NAVIGATION_PATTERN.match(segment)
            if not match: 
                continue

            key, array_spec = match.groups()

            if not array_spec:
                if key not in current or not isinstance(current[key], dict): 
                    current[key] = {}
                current = current[key]
            elif re.match(r'\[\d+\]', array_spec):
                index = int(array_spec.strip('[]'))
                if key not in current or not isinstance(current[key], list): 
                    current[key] = []
                
                arr = current[key]
                while len(arr) <= index: 
                    arr.append({})

                if not isinstance(arr[index], dict): 
                    arr[index] = {}
                current = arr[index]
            else:
                raise ValueError(f"Path di destinazione malformato: '{path}'. Non usare '[]' prima della chiave finale.")
        return current

    def _set_value_at_path(self, doc: JsonDocument, path: str, value: Any):
        """Imposta un valore al path specificato nel documento."""
        if not path:
            return
            
        segments = path.split('.')
        current = doc

        for i, segment in enumerate(segments):
            match = self._NAVIGATION_PATTERN.match(segment)
            if not match: 
                continue

            key, array_spec = match.groups()
            is_last = (i == len(segments) - 1)

            if not array_spec:
                if is_last: 
                    current[key] = value
                else:
                    if key not in current or not isinstance(current[key], dict): 
                        current[key] = {}
                    current = current[key]
            
            elif re.match(r'\[\d+\]', array_spec):
                index = int(array_spec.strip('[]'))
                
                if key not in current or not isinstance(current[key], list): 
                    current[key] = []

                arr = current[key]
                is_sub_path = (i < len(segments) - 1)

                while len(arr) <= index: 
                    arr.append({} if is_sub_path or isinstance(value, dict) else None) 

                if is_last:
                    if isinstance(arr[index], dict) and isinstance(value, dict):
                         arr[index].update(value)
                    else:
                         arr[index] = value 
                else:
                    if not isinstance(arr[index], dict): 
                        arr[index] = {}
                    current = arr[index]
            else:
                raise ValueError(f"Path di destinazione non supportato: '{path}'. Evitare '[]' in assegnazione diretta.")
    
    def _is_source_path(self, arg: Any) -> bool:
        if not isinstance(arg, str) or not arg.strip():
            return False

        if arg in self._valid_source_paths or arg in self._field_mapping:
            return True

        # Valida path[N] arbitrario se path[] è noto nello schema
        match = re.match(r'^(.+)\[(\d+)\](.*)$', arg)
        if match:
            base, _, suffix = match.groups()
            generalized = f"{base}[]{suffix}"
            if generalized in self._valid_source_paths:
                return True

        return False

    def _resolve_argument(self, arg: Any) -> Any:
        """
        Risolve un singolo argomento: se è un path sorgente valido, estrae il valore,
        altrimenti restituisce l'argomento come valore fisso.
        
        Restituisce SEMPRE la lista completa dei valori trovati.
        """
        #print(f"DEBUG _resolve_argument: resolving '{arg}' (type: {type(arg)})")
        
        if not isinstance(arg, str):
            #print(f"DEBUG _resolve_argument: non-string arg '{arg}' -> FIXED VALUE")
            return arg
            
        if not self._is_source_path(arg):
            # Verifica se sembra un path ma non è valido nello schema
            if '.' in arg or '[' in arg or ']' in arg:
                #print(f"DEBUG _resolve_argument: '{arg}' looks like a path but not valid -> FIXED VALUE")
                return arg
            else:
                #print(f"DEBUG _resolve_argument: '{arg}' -> FIXED VALUE")
                return arg
        
        # È un path sorgente valido - risolvi dal documento
        resolved_values = self._get_values_from_path(self._current_src_doc, arg)
        
        # La funzione di trasformazione dovrebbe gestire le liste
        if resolved_values:
            #print(f"DEBUG _resolve_argument: '{arg}' -> EXTRACTED VALUES {resolved_values} (type: {type(resolved_values)}, count: {len(resolved_values)})")
            return resolved_values
        else:
            #print(f"DEBUG _resolve_argument: '{arg}' -> NO VALUES FOUND")
            return []

    def _process_operation(self, operation: List[Any]) -> List[Any]:
        """
        Applica l'operazione di trasformazione ai valori sorgente.
        Gestisce correttamente gli argomenti che sono liste di valori.
        """
        if not operation:
            return []

        op_spec = operation[0]  # Nome della funzione
        op_args = operation[1:] if len(operation) > 1 else []  # Argomenti della funzione

        logger.info(f"DEBUG _process_operation: {op_spec} with args: {op_args}")

        # Caso: funzione registrata
        if op_spec in self._function_registry:
            func = self._function_registry[op_spec]
            resolved_args = []

            for arg in op_args:
                resolved_arg = self._resolve_argument(arg)
                # Unwrap single-element result sets from path resolution
                # e.g. [["chunk1","chunk2"]] -> ["chunk1","chunk2"]
                # e.g. ["some string"] -> "some string"
                if isinstance(resolved_arg, list) and len(resolved_arg) == 1:
                    resolved_args.append(resolved_arg[0])
                else:
                    resolved_args.append(resolved_arg)

            logger.info(f"DEBUG: Calling {op_spec} with resolved args: {resolved_args} (types: {[type(arg) for arg in resolved_args]})")

            # Esecuzione della Funzione - SEMPRE con op_spec come primo argomento
            try:
                # Se un argomento è una lista, potremmo dover chiamare la funzione multiple volte
                # o passare la lista come singolo argomento, dipende dalla funzione
                
                # Per semplicità, passiamo gli argomenti così come sono
                # La funzione di trasformazione dovrebbe gestire liste di valori
                transformed_value = func(op_spec, *resolved_args)
                
                # Normalizziamo i casi in cui la funzione ritorna None: restituiamo []
                if transformed_value is None:
                    return []
                elif isinstance(transformed_value, list):
                    return transformed_value
                else:
                    # Per qualsiasi altro tipo (dict, int, float, bool, str), restituiscilo come lista con un elemento
                    return [transformed_value]

            except Exception as e:
                error_msg = f"Errore di esecuzione funzione '{op_spec}' con argomenti {op_args}: {e}"
                self.errors.append(error_msg)
                # Se una UDF fallisce, restituiamo una lista vuota come fallback
                return []

        # Caso: semplice path sorgente (nessuna funzione) - es: ["field_name"]
        elif isinstance(op_spec, str) and self._is_source_path(op_spec):
            real_src_values = self._get_values_from_path(self._current_src_doc, op_spec)
            logger.info(f"Simple path resolution: {op_spec} -> {real_src_values}")
            return real_src_values

        # Caso: valore fisso diretto - es: ["fixed_value"]
        elif isinstance(op_spec, str):
            logger.info(f"Fixed value: {op_spec}")
            return [op_spec]

        else:
            logger.info(f"Fixed non-string value: {op_spec}")
            return [op_spec]

    def _process_mapping_entry(self, dst_path: str, operation: List[Any], dst_doc: JsonDocument):
        """Elabora una singola entry di mapping."""
        logger.info(f"Processing mapping entry: {dst_path} -> {operation}")
        transformed_values = self._process_operation(operation)
        logger.info(f"Transformed values for {dst_path}: {transformed_values}")

        if '[]' in dst_path:
            try:
                base_path_match = re.match(r'(.+?)\[\](.*)', dst_path)
                if not base_path_match: 
                    raise ValueError("Formato array moltiplicatore non riconosciuto.")
                
                base_path = base_path_match.group(1).rstrip('.')
                suffix = base_path_match.group(2).strip('.')
                
            except ValueError as e:
                self.errors.append(f"Path di destinazione malformato '{dst_path}': {e}")
                return

            path_segments = base_path.split('.')
            array_key = path_segments[-1] if path_segments else None
            parent_path = ".".join(path_segments[:-1])
            
            if not array_key: 
                return

            parent_container = dst_doc
            if parent_path: 
                parent_container = self._get_parent_container(dst_doc, parent_path)

            if array_key not in parent_container or not isinstance(parent_container.get(array_key), list):
                parent_container[array_key] = []
            
            target_array = parent_container[array_key]

            for i, value in enumerate(transformed_values):
                while len(target_array) <= i: 
                    if suffix:
                        target_array.append({})
                    elif isinstance(value, dict):
                        target_array.append({})
                    elif isinstance(value, list):
                        target_array.append([])
                    else:
                        target_array.append(None)
                
                if not suffix:
                    target_array[i] = value
                else:
                    if not isinstance(target_array[i], dict): 
                        target_array[i] = {}
                    self._set_value_at_path(target_array[i], suffix, value)

        else:
            value_to_set = transformed_values[0] if transformed_values else None
            self._set_value_at_path(dst_doc, dst_path, value_to_set)

    def apply_mapping(self, src_doc: JsonDocument) -> Tuple[JsonDocument | None, bool, List[str]]:
        """
        Applica l'intero mapping con validazione.
        """
        logger.info("Applying mapping to  {} ...".format(src_doc))
        self.errors = []
        
        logger.info(f"Applying mapping to source document: {src_doc}")
        
        # Validazione Sorgente
        '''
        is_source_valid = self.validate_source(src_doc)
        #print(f"Source validation: {'PASS' if is_source_valid else 'FAIL'}")
        '''
        dst_doc: JsonDocument = {}
        self._current_src_doc = src_doc 

        # Applicazione del Mapping
        logger.info("Starting mapping process...")
        for dst_path, operation in self.mapping_spec.items():
            try:
                logger.info(f"Mapping: {dst_path} <- {operation}")
                self._process_mapping_entry(dst_path, operation, dst_doc)
                logger.info(f"After mapping {dst_path}, document: {dst_doc}")
            except Exception as e:
                error_message = f"Errore di Mapping per '{dst_path}': {e} (Operazione: {operation})"
                logger.error(error_message)
                self.errors.append(error_message)

        self._current_src_doc = None
        
        # Validazione Destinazione
        is_destination_valid = self.validate_destination(dst_doc)
        logger.info(f"Destination validation: {'PASS' if is_destination_valid else 'FAIL'}")
        logger.info(f"Final document: {dst_doc}")
        
        #success = is_source_valid and is_destination_valid and not self.errors
        success = not self.errors and is_destination_valid
        
        if is_destination_valid or not self.errors:
            return dst_doc, success, self.errors
        else:
            return None, success, self.errors

