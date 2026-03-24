"""
Python Function Validator for User Defined Functions (UDF)
=====================================================================================

Supports multiple parameters of different types (str, int, float, list, dict)
while maintaining the requirement that the first parameter is always func_name: str

Output: return type can be list[str] OR str
"""

import ast
from typing import Dict, Any, Optional

FN_PLACEHOLDER = '''
def user_defined_function(func_name: str, **kwargs) -> Union[list[str], str]:
    """
    func_name: always required as the first parameter (str)
    **kwargs: optional additional parameters named param_[i] (int, float, list, dict)
    """
    # Implement your logic here #
    # Now you can return a list of strings OR a single string
    return ["result1", "result2"]
'''

# ============================================================================
# 1. MAIN VALIDATION FUNCTION
# ============================================================================

def validate_user_function(user_code: str, func_name: Optional[str] = None) -> Dict[str, Any]:
    """
    Validates that the user code is a valid Python function with specific requirements.

    UPDATED REQUIREMENTS:
    - First parameter must be: func_name: str
    - Return type must be: list[str], List[str], OR str
    - Correct syntax and indentation

    Args:
        user_code (str): Python code to validate
        func_name (Optional[str]): Specific function name to search for (optional)

    Returns:
        Dict with:
            - is_valid (bool): True if the function is valid
            - errors (List[str]): List of errors found
            - function_name (Optional[str]): Name of the function found
            - warnings (List[str]): Non-blocking warnings
            - return_type (Optional[str]): Detected return type ('list' or 'str')
    """
    result = {
        'is_valid': False,
        'errors': [],
        'warnings': [],
        'function_name': None,
        'ast_tree': None,
        'return_type': None  # New field to indicate the return type
    }
    
    # ------------------------------------------------------------------------
    # PHASE 1: Basic syntactic validation with AST
    # ------------------------------------------------------------------------
    
    if not user_code or not user_code.strip():
        result['errors'].append("The code is empty")
        return result
    
    code_str = user_code.strip()
    
    try:
        tree = ast.parse(code_str)
        result['ast_tree'] = tree
    except SyntaxError as e:
        result['errors'].append(f"Syntax error at line {e.lineno}: {e.msg}")
        return result
    except IndentationError as e:
        result['errors'].append(f"Indentation error at line {e.lineno}: {e.msg}")
        return result
    except Exception as e:
        result['errors'].append(f"Error during code parsing: {str(e)}")
        return result
    
    # ------------------------------------------------------------------------
    # PHASE 2: Search for function definitions in the AST
    # ------------------------------------------------------------------------
    
    function_defs = [node for node in ast.walk(tree) if isinstance(node, ast.FunctionDef)]
    
    if not function_defs:
        result['errors'].append("The code does not contain function definitions (missing 'def function_name:')")
        return result
    
    if func_name:
        matching_funcs = [f for f in function_defs if f.name == func_name]
        if not matching_funcs:
            result['errors'].append(f"No function named '{func_name}' found in the code")
            return result
        target_func = matching_funcs[0]
    else:
        if len(function_defs) > 1:
            result['warnings'].append(
                f"Found {len(function_defs)} functions. '{function_defs[0].name}' will be used. "
                f"Specify a name if you want to validate a different one."
            )
        target_func = function_defs[0]
    
    result['function_name'] = target_func.name
    
    # ------------------------------------------------------------------------
    # PHASE 3: Specific requirements validation
    # ------------------------------------------------------------------------
    
    # 3.1 FIRST PARAMETER CHECK: must be 'func_name: str'
    
    if not target_func.args.args:
        result['errors'].append("The function has no parameters. It must have at least 'func_name: str' as the first parameter")
    else:
        first_arg = target_func.args.args[0]
        
        if first_arg.arg != 'func_name':
            result['errors'].append(
                f"The first parameter must be named 'func_name', not '{first_arg.arg}'. "
                f"Example: def {target_func.name}(func_name: str) -> ..."
            )
        
        if hasattr(first_arg, 'annotation') and first_arg.annotation:
            try:
                if hasattr(ast, 'unparse'):
                    ann_str = ast.unparse(first_arg.annotation)
                else:
                    ann_str = ast.dump(first_arg.annotation)
                
                normalized = ann_str.replace(' ', '').replace('builtins.', '').replace("'", "").replace('"', '')
                
                if 'str' not in normalized:
                    result['errors'].append(
                        f"The first parameter must have type 'str', not '{ann_str}'. "
                        f"Example: def {target_func.name}(func_name: str) -> ..."
                    )
            except:
                result['warnings'].append(
                    "Unable to verify the type hint of the first parameter. "
                    "Make sure it is 'func_name: str'"
                )
        else:
            result['warnings'].append(
                f"Missing type hint for the first parameter. "
                f"It should be: def {target_func.name}(func_name: str) -> ..."
            )
    
    # 3.2 RETURN TYPE CHECK: MODIFIED to support 'list[str]' OR 'str'
    
    if target_func.returns:
        try:
            if hasattr(ast, 'unparse'):
                return_str = ast.unparse(target_func.returns)
            else:
                return_str = ast.dump(target_func.returns)
            
            normalized_return = (
                return_str
                .replace(' ', '')
                .replace('builtins.', '')
                .replace('typing.', '')
                .replace("'", "")
                .replace('"', '')
                .lower()
            )
            
            # Valid return types (updated)
            valid_list_return = any(
                valid in normalized_return 
                for valid in ['list[str]', 'list[ str ]', 'list [ str ]']
            )
            
            valid_str_return = any(
                valid in normalized_return 
                for valid in ['str', 'string', 'builtins.str']
            )
            
            # Check Union types
            if 'union' in normalized_return or '|' in normalized_return:
                # Supports Union[list[str], str] or list[str] | str
                has_list = any(valid in normalized_return for valid in ['list[str]', 'list[ str ]'])
                has_str = any(valid in normalized_return for valid in ['str', 'string'])
                if has_list or has_str:
                    result['return_type'] = 'union'  # Can be list or string
                else:
                    result['errors'].append(
                        f"The Union return type must include 'list[str]' and/or 'str'. Found: '{return_str}'"
                    )
            
            elif valid_list_return:
                result['return_type'] = 'list'
            elif valid_str_return:
                result['return_type'] = 'str'
            else:
                result['errors'].append(
                    f"The return type must be 'list[str]', 'List[str]', OR 'str', not '{return_str}'. "
                    f"Example: def {target_func.name}(func_name: str) -> list[str]:"
                    f"Or: def {target_func.name}(func_name: str) -> str:"
                )
        except:
            result['warnings'].append(
                "Unable to verify the return value type hint. "
                "Make sure it is '-> list[str]' or '-> str'"
            )
    else:
        result['warnings'].append(
            f"Missing type hint for the return value. "
            f"It should be: def {target_func.name}(func_name: str) -> list[str]:"
            f"Or: def {target_func.name}(func_name: str) -> str:"
        )
    
    # 3.3 SECURITY CHECK (unchanged)
    
    dangerous_operations = []
    
    for node in ast.walk(target_func):
        if isinstance(node, ast.Import):
            for alias in node.names:
                dangerous_operations.append(f"import {alias.name}")
        
        elif isinstance(node, ast.ImportFrom):
            module = node.module or "unknown"
            dangerous_operations.append(f"from {module} import ...")
        
        elif isinstance(node, ast.Call):
            try:
                if hasattr(ast, 'unparse'):
                    call_str = ast.unparse(node)
                else:
                    call_str = ast.dump(node)
                
                dangerous_functions = ['eval', 'exec', 'compile', '__import__', 'open', 
                                      'os.system', 'subprocess', 'popen', 'input']
                
                if any(func in call_str.lower() for func in dangerous_functions):
                    dangerous_operations.append(f"risky function call: {call_str[:50]}...")
            except:
                pass
    
    if dangerous_operations:
        result['warnings'].append(
            f"Found {len(dangerous_operations)} potentially dangerous operations. "
            f"Make sure you trust this code."
        )
        result['warnings'].extend(dangerous_operations[:3])
    
    # ------------------------------------------------------------------------
    # PHASE 4: Final validity determination
    # ------------------------------------------------------------------------
    
    result['is_valid'] = len(result['errors']) == 0
    
    return result

# ============================================================================
# 2. SAFE EXECUTION FUNCTION WITH MULTIPLE PARAMETERS
# ============================================================================

def execute_user_function_safely(
    user_code: str, 
    func_name: str, 
    params: Dict[str, Any],
    expected_return_type: Optional[str] = None  # New optional parameter
) -> Dict[str, Any]:
    """
    Executes a user function in a restricted environment (sandbox).
    Supports multiple parameters of different types.

    UPDATED: Supports both list[str] and str as return type.

    Args:
        user_code (str): Python code with the function
        func_name (str): Name of the function to execute
        params (Dict[str, Any]): Dictionary of parameters to pass to the function.
                                 MUST contain 'func_name' as the first key with a str value.
        expected_return_type (Optional[str]): Expected return type ('list', 'str', or 'union').
                                              If None, verifies automatically.

    Returns:
        Dict with:
            - success (bool): True if execution succeeded
            - result (Any): Result of the function
            - return_type (str): Actual type of the result ('list' or 'str')
            - error (Optional[str]): Error message if it fails
            - execution_time (float): Execution time in seconds
    """
    import time
    
    result = {
        'success': False,
        'result': None,
        'return_type': None,  # New field
        'error': None,
        'execution_time': 0.0
    }
    
    start_time = time.time()
    
    try:
        # --------------------------------------------------------------------
        # VALIDAZIONE PARAMETRI
        # --------------------------------------------------------------------
        
        # 1. Verifica che params sia un dizionario
        if not isinstance(params, dict):
            result['error'] = f"'params' deve essere un dizionario, non {type(params).__name__}"
            return result
        
        # 2. Verifica che 'func_name' sia presente
        if 'func_name' not in params:
            result['error'] = "Il parametro obbligatorio 'func_name' non è presente in params"
            return result
        
        # 3. Verifica che 'func_name' sia una stringa
        if not isinstance(params['func_name'], str):
            result['error'] = f"Il parametro 'func_name' deve essere str, non {type(params['func_name']).__name__}"
            return result
        
        # --------------------------------------------------------------------
        # AMBIENTE DI ESECUZIONE LIMITATO PER SICUREZZA
        # --------------------------------------------------------------------
        safe_builtins = {
            # Funzioni base sicure
            'str': str, 'int': int, 'float': float, 'bool': bool,
            'len': len, 'range': range, 'enumerate': enumerate,
            'list': list, 'dict': dict, 'tuple': tuple, 'set': set,
            'isinstance': isinstance, 'type': type,
            'min': min, 'max': max, 'sum': sum, 'abs': abs,
            'round': round, 'sorted': sorted, 'reversed': reversed,
            'zip': zip, 'map': map, 'filter': filter,
            'any': any, 'all': all,
            
            # Costanti
            'True': True, 'False': False, 'None': None,
        }
        
        safe_globals = {
            '__builtins__': safe_builtins,
            '__name__': '__main__',
        }
        
        # --------------------------------------------------------------------
        # ESECUZIONE
        # --------------------------------------------------------------------
        
        # 1. Esegui il codice per definire la funzione nell'ambiente sicuro
        exec(user_code, safe_globals)
        
        # 2. Verifica che la funzione esista
        if func_name not in safe_globals:
            result['error'] = f"Funzione '{func_name}' non trovata dopo l'esecuzione del codice"
            return result
        
        user_func = safe_globals[func_name]
        
        # 3. Verifica che sia effettivamente una funzione
        if not callable(user_func):
            result['error'] = f"'{func_name}' non è una funzione chiamabile"
            return result
        
        # 4. Esegui la funzione con tutti i parametri
        func_result = user_func(**params)
        
        # 5. VERIFICA TIPO DI RITORNO (MODIFICATA per supportare str)
        
        # Se abbiamo un tipo atteso dalla validazione, usalo
        if expected_return_type == 'list':
            # Verifica che sia una lista
            if not isinstance(func_result, list):
                result['error'] = f"Il risultato non è una lista come atteso, ma {type(func_result).__name__}"
                return result
            
            # Verifica che tutti gli elementi siano stringhe (se la lista non è vuota)
            if func_result and not all(isinstance(item, str) for item in func_result):
                non_string_items = [type(item).__name__ for item in func_result if not isinstance(item, str)]
                result['error'] = f"Non tutti gli elementi sono stringhe. Tipi trovati: {set(non_string_items)}"
                return result
            
            result['return_type'] = 'list'
            
        elif expected_return_type == 'str':
            # Verifica che sia una stringa
            if not isinstance(func_result, str):
                result['error'] = f"Il risultato non è una stringa come atteso, ma {type(func_result).__name__}"
                return result
            
            result['return_type'] = 'str'
            
        elif expected_return_type == 'union':
            # Accetta sia lista che stringa
            if isinstance(func_result, list):
                # Se è una lista, verifica che gli elementi siano stringhe
                if func_result and not all(isinstance(item, str) for item in func_result):
                    non_string_items = [type(item).__name__ for item in func_result if not isinstance(item, str)]
                    result['error'] = f"Se ritorni una lista, tutti gli elementi devono essere stringhe. Tipi trovati: {set(non_string_items)}"
                    return result
                result['return_type'] = 'list'
            elif isinstance(func_result, str):
                result['return_type'] = 'str'
            else:
                result['error'] = f"Il risultato deve essere list[str] o str, non {type(func_result).__name__}"
                return result
                
        else:
            # Auto-rilevamento del tipo (backward compatibility)
            if isinstance(func_result, list):
                # Verifica che tutti gli elementi siano stringhe
                if func_result and not all(isinstance(item, str) for item in func_result):
                    non_string_items = [type(item).__name__ for item in func_result if not isinstance(item, str)]
                    result['error'] = f"Non tutti gli elementi sono stringhe. Tipi trovati: {set(non_string_items)}"
                    return result
                result['return_type'] = 'list'
            elif isinstance(func_result, str):
                result['return_type'] = 'str'
            else:
                result['error'] = f"Il risultato deve essere list[str] o str, non {type(func_result).__name__}"
                return result
        
        # Successo!
        result['success'] = True
        result['result'] = func_result
        
    except TypeError as e:
        # Errore di tipo nei parametri
        result['error'] = f"Errore nei parametri della funzione: {str(e)}"
    except Exception as e:
        result['error'] = f"Errore durante l'esecuzione: {str(e)}"
    
    finally:
        result['execution_time'] = time.time() - start_time
    
    return result

# ============================================================================
# 3. FUNZIONE PRINCIPALE PER INTEGRAZIONE CON PARAMETRI MULTIPLI 
# ============================================================================

def validate_and_execute_user_query(
    user_code: str, 
    expected_func_name: Optional[str] = None,
    params: Dict[str, Any] = None
) -> Dict[str, Any]:
    """
    Funzione completa per validare ed eseguire una query utente.
    Supporta parametri multipli di tipi diversi.
    
    AGGIORNATO: Supporta sia list[str] che str come tipo di ritorno.
    
    Args:
        user_code (str): Codice Python inserito dall'utente
        expected_func_name (Optional[str]): Nome atteso della funzione
        params (Dict[str, Any]): Dizionario parametri. DEVE contenere 'func_name' come chiave.
                                 Se None, usa {'func_name': 'user_query'}
    
    Returns:
        Dict con tutti i dettagli di validazione ed esecuzione
        
    Example:
        >>> result = validate_and_execute_user_query(
        ...     user_code=code,
        ...     params={
        ...         'func_name': 'search query',
        ...         'max_results': 10,
        ...         'filters': ['stop']
        ...     }
        ... )
    """
    # Parametri di default se non specificati
    if params is None:
        params = {'func_name': 'user_query'}
    
    # 1. Validazione del codice
    validation_result = validate_user_function(user_code, expected_func_name)
    
    if not validation_result['is_valid']:
        return {
            'success': False,
            'stage': 'validation',
            'errors': validation_result['errors'],
            'warnings': validation_result['warnings'],
            'function_name': validation_result['function_name'],
            'return_type': validation_result.get('return_type'),
            'result': None,
            'execution_time': 0.0
        }
    
    # 2. Determina il nome della funzione
    func_name = validation_result['function_name'] or expected_func_name
    if not func_name:
        return {
            'success': False,
            'stage': 'execution',
            'errors': ["Nome della funzione non determinato"],
            'warnings': validation_result['warnings'],
            'function_name': None,
            'return_type': validation_result.get('return_type'),
            'result': None,
            'execution_time': 0.0
        }
    
    # 3. Esecuzione in sandbox (solo se la validazione ha successo)
    execution_result = execute_user_function_safely(
        user_code, 
        func_name, 
        params,
        expected_return_type=validation_result.get('return_type')  # Passa il tipo atteso
    )
    
    # 4. Combina i risultati
    final_result = {
        'success': execution_result['success'],
        'stage': 'execution' if execution_result['success'] else 'execution_error',
        'errors': [execution_result['error']] if execution_result['error'] else [],
        'warnings': validation_result['warnings'],
        'function_name': func_name,
        'return_type': execution_result.get('return_type'),  # Tipo effettivo restituito
        'expected_return_type': validation_result.get('return_type'),  # Tipo atteso dalla validazione
        'result': execution_result['result'],
        'execution_time': execution_result['execution_time']
    }
    
    return final_result

