import os
import traceback
from pathlib import Path
from typing import Optional, List, Dict, Any
import pyarrow.parquet as pq
import duckdb
import numpy as np

import logging
import plotly.express as px

from utils.path_utils import to_binded_path, to_internal_path
from utils.streamlit_func import reset_dashboard_session_state
from utils.extract_glob import generate_filtered_globs

from data_class.repository.table.distribution_repository import DistributionRepository
from data_class.repository.table.dataset_repository import DatasetRepository
from data_class.entity.table.distribution import Distribution
from data_class.entity.table.dataset import Dataset

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Configurazione ambiente

MAPPED_DATA_DIR = os.getenv("MAPPED_DATA_DIR")
STATS_DATA_DIR = os.getenv("STATS_DATA_DIR")
BINDED_MAPPED_DATA_DIR = os.getenv("BINDED_MAPPED_DATA_DIR")
BINDED_STATS_DATA_DIR = os.getenv("BINDED_STATS_DATA_DIR")

LOW_LEVEL_STATS_EXTENSION = os.getenv("LOW_LEVEL_STATS_EXTENSION")

BASE_PREFIX = os.getenv("BASE_PREFIX")

from config.state_vars import distribution_keys,home_vars
KEYS_TO_KEEP = distribution_keys + home_vars

### AUXILIARY FUNCTIONS ###

def _initialize_repositories(st_app):
    """Inizializza i repository necessari."""
    return {
        'distribution': DistributionRepository(st_app.session_state.db_manager),
        'dataset': DatasetRepository(st_app.session_state.db_manager)
    }

def _compact_sql_query(query: str) -> str:
    """Rimuove spazi multipli, a capo e tabulazioni da una query SQL."""
    import re
    query = query.replace('\n', ' ').replace('\r', ' ').replace('\t', ' ')
    query = re.sub(r'\s+', ' ', query)
    return query.strip()

def _get_file_extension(distribution_path: str) -> Optional[str]:
    """Recupera l'estensione del file, gestendo anche estensioni composte."""
    try:
        first_file = next((entry.name for entry in os.scandir(distribution_path) if entry.is_file()), None)
        if not first_file:
            return None
        
        parts = first_file.split('.')
        if len(parts) < 2:
            return None
        
        compression_exts = {'gz', 'bz2', 'xz', 'zip', '7z', 'rar', 'zst', 'tgz', 'tbz2'}
        
        if parts[-1].lower() in compression_exts and len(parts) > 2:
            return '.' + '.'.join(parts[-2:])
        
        return '.' + parts[-1]
    except Exception:
        return None

def _get_distribution_paths(distribution: Distribution) -> tuple:
    """Ottiene i path per i dati e le statistiche della distribution."""
    data_path = distribution.uri.replace(BASE_PREFIX, '')
    stats_path = data_path.replace(BINDED_MAPPED_DATA_DIR, BINDED_STATS_DATA_DIR) + LOW_LEVEL_STATS_EXTENSION
    return to_internal_path(data_path), to_internal_path(stats_path)

### STATS EXTRACTION AND CALCULATION FUNCTIONS ###

def _extract_stats_columns(stats_path: str) -> List[str]:
    """Estrae dinamicamente le colonne dalle statistiche leggendo il primo file parquet."""
    try:
        stats_files = list(Path(stats_path).glob('*.parquet'))
        if not stats_files:
            logger.warning(f"No parquet files found in {stats_path}")
            return []
        
        # Leggi schema del primo file
        table = pq.read_table(stats_files[0])
        columns = [col for col in table.column_names if col != 'id']  # Escludi 'id' dalla lista
        return columns
    except Exception as e:
        logger.error(f"Error extracting stats columns: {e}")
        return []

def _calculate_column_stats(stats_path: str, column: str) -> Dict[str, Any]:
    """Calcola statistiche descrittive per una colonna usando DuckDB."""
    try:
        conn = duckdb.connect(':memory:')
        query = f"""
            SELECT 
                MIN({column}) as min_val,
                MAX({column}) as max_val,
                AVG({column}) as mean_val,
                MEDIAN({column}) as median_val,
                PERCENTILE_CONT(0.05) WITHIN GROUP (ORDER BY {column}) as p05,
                PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY {column}) as p95,
                STDDEV({column}) as std_dev,
                COUNT({column}) as count_val
            FROM read_parquet('{stats_path}/*.parquet')
            WHERE {column} IS NOT NULL
        """
        result = conn.execute(query).fetchone()
        conn.close()
        
        if result:
            return {
                'min': result[0],
                'max': result[1],
                'mean': result[2],
                'median': result[3],
                'p05': result[4],
                'p95': result[5],
                'std': result[6],
                'count': result[7]
            }
        return {}
    except Exception as e:
        logger.error(f"Error calculating stats for {column}: {e}")
        return {}

def _calculate_skewness_kurtosis(data: np.ndarray) -> tuple:
    """Calcola skewness e kurtosis da array numpy."""
    try:
        from scipy import stats
        skewness = stats.skew(data, nan_policy='omit')
        kurtosis = stats.kurtosis(data, nan_policy='omit')
        return skewness, kurtosis
    except Exception as e:
        logger.warning(f"Error calculating skewness/kurtosis: {e}")
        return None, None

def _get_column_data(stats_path: str, column: str, sample_size: int = 100000) -> np.ndarray:
    """Recupera i dati di una colonna per visualizzazione."""
    try:
        conn = duckdb.connect(':memory:')
        query = f"""
            SELECT {column}
            FROM read_parquet('{stats_path}/*.parquet')
            WHERE {column} IS NOT NULL
            LIMIT {sample_size}
        """
        result = conn.execute(query).fetchall()
        conn.close()
        return np.array([r[0] for r in result])
    except Exception as e:
        logger.error(f"Error fetching column data: {e}")
        return np.array([])

def _render_stats_visualization(st_app, stats_path: str, stats_columns: List[str]):
    """Renderizza l'interfaccia di visualizzazione delle statistiche."""
    st_app.markdown("---")
    st_app.subheader("📊 Visualizzazione Statistiche")
    
    if not stats_columns:
        st_app.warning("⚠️ Nessuna colonna di statistiche trovata")
        return
    
    with st_app.expander("ℹ️ Info Low Level Document Stats", expanded=False):
        st_app.markdown("""
            ### 📜 Low Level Stats (Document Level)
            
            | Metrica | Descrizione |
            | :--- | :--- |
            | **Statistiche Righe (Lines)** | |
            | `_lines_count` | Numero totale di righe nel documento |
            | `_lines_empty_count` | Numero di righe vuote |
            | `_lines_short_char_20_count` | Righe molto corte (< 20 caratteri) |
            | `_lines_long_char_10000_count` | Righe estremamente lunghe (> 10.000 caratteri) |
            | `_lines_end_punctuation_count` | Righe che terminano con punteggiatura finale |
            | `_lines_bulletpoint_start_count`| Righe che iniziano con un bullet point |
            | `_lines_end_ellipsis_count` | Righe che terminano con puntini di sospensione |
            | **Statistiche Parole (Words & Tokens)** | |
            | `_word_count` | Conteggio totale parole (split spazi) |
            | `_word_distinct_count` | Numero di parole uniche (vocabolario) |
            | `_token_count` | Stima dei token basata sulla fertilità linguistica |
            | `_stop_word_ratio` | Rapporto tra stopwords e parole totali |
            | `_no_alpha_count` | Parole che non contengono caratteri alfabetici |
            | `_mean_word_length` | Lunghezza media delle parole |
            | `_unigram_entropy` | Entropia del testo (misura di diversità/caos) |
            | **Statistiche Caratteri (Chars)** | |
            | `_char_count` | Numero totale di caratteri (incluso spazi) |
            | `_char_distinct_count` | Numero di caratteri unici utilizzati |
            | `_char_symbol_count` | Conteggio simboli speciali (#, $, %, etc.) |
            | `_numerical_char_count` | Conteggio dei caratteri numerici (0-9) |
            | `_char_uppercase_count` | Conteggio dei caratteri maiuscoli |
            | `_char_escape_count` | Conteggio caratteri di escape (\\n, \\t, \\r) |
            | **Rapporti e Qualità (Ratios)** | |
            | `_white_space_ratio` | Percentuale di spazi bianchi sul totale |
            | `_non_alpha_digit_ratio` | Rapporto caratteri non alfanumerici |
            | `_punctuation_ratio` | Rapporto punteggiatura totale sul testo |
            | `_symbol_word_ratio` | Rapporto tra simboli e numero di parole |
            | `_unique_word_ratio` | Rapporto tra parole uniche e totali |
            | `_javascript_count` | Occorrenze della keyword 'javascript' (code leak detection) |
            """)

    # Selectbox per scegliere la colonna
    selected_column = st_app.selectbox(
        "Seleziona una statistica da visualizzare:",
        options=stats_columns
    )
    
    if selected_column:
        _render_column_visualization(st_app, stats_path, selected_column)

def _render_column_visualization(st_app, stats_path: str, col_name: str):
    """
    Renderizza la visualizzazione completa dei dati della colonna selezionata:
    - Istogramma
    - Statistiche descrittive (min, max, mean, median, 5%, 95%, std)
    - Skewness e Kurtosis
    """
    st_app.markdown("---")
    st_app.markdown(f"### 📈 Analisi: `{col_name}`")
    
    with st_app.spinner("Caricamento dati e calcolo statistiche..."):
        # Calcola statistiche descrittive
        stats = _calculate_column_stats(stats_path, col_name)
        
        if not stats:
            st_app.error("❌ Impossibile calcolare le statistiche per questa colonna")
            return
        
        # Recupera dati per istogramma e calcolo skewness/kurtosis
        data = _get_column_data(stats_path, col_name)
        
        if len(data) == 0:
            st_app.warning("⚠️ Nessun dato disponibile per questa colonna")
            return
        
        # Calcola skewness e kurtosis
        skewness, kurtosis = _calculate_skewness_kurtosis(data)
        
        # Layout a due colonne
        col1, col2 = st_app.columns([2, 1])
        
        with col1:
            # Istogramma con Plotly
            st_app.markdown("#### 📊 Histogram")
            fig = px.histogram(
                x=data,
                nbins=50,
                title=f"Distribuzione di {col_name}",
                labels={'x': col_name, 'y': 'Frequenza'}
            )
            fig.update_layout(
                showlegend=False,
                height=400,
                template="plotly_white"
            )
            st_app.plotly_chart(fig, use_container_width=True)

            subcol1, subcol2 = st_app.columns(2)
            #skewness e kurtosis
            if skewness is not None:
                with subcol1:
                    st_app.metric(
                        "🔀 Skewness",
                        f"{skewness:.4f}",
                        help="Misura l'asimmetria della distribuzione. 0=simmetrica, >0=coda destra, <0=coda sinistra"
                    )
            if kurtosis is not None:
                with subcol2:
                    st_app.metric(
                        "📊 Kurtosis",
                        f"{kurtosis:.4f}",
                        help="Misura la 'pesantezza' delle code. 0=normale, >0=code pesanti, <0=code leggere"
                    )
        
        with col2:
            st_app.markdown("#### 📐 Descriptions")
            subcol1, subcol2 = st_app.columns(2)
            metrics_data = [
                ("📊 Count", f"{stats['count']:,}"),
                ("📉 Min", f"{stats['min']:.4f}"),
                ("📈 Max", f"{stats['max']:.4f}"),
                ("⭐ Mean", f"{stats['mean']:.4f}"),
                ("📍 5th %ile", f"{stats['p05']:.4f}"),
                ("📍 95th %ile", f"{stats['p95']:.4f}"),
                ("📏 Std Dev", f"{stats['std']:.4f}"),
                ("🎯 Median", f"{stats['median']:.4f}"),
            ]
            with subcol1:
                for label, value in metrics_data[:4]:
                    st_app.metric(label, value)
            with subcol2:
                for label, value in metrics_data[4:]:
                    st_app.metric(label, value)

## QUERY BUILDER AND EXECUTION FUNCTIONS ###

def _build_filter_condition(column: str, filter_type: str, value: Any) -> str:
    """Costruisce una condizione SQL dal filtro."""
    if filter_type == '>':
        return f"s.{column} > {value}"
    elif filter_type == '<':
        return f"s.{column} < {value}"
    elif filter_type == '>=':
        return f"s.{column} >= {value}"
    elif filter_type == '<=':
        return f"s.{column} <= {value}"
    elif filter_type == '=':
        return f"s.{column} = {value}"
    elif filter_type == '!=':
        return f"s.{column} != {value}"
    elif filter_type == 'range':
        return f"s.{column} BETWEEN {value[0]} AND {value[1]}"
    elif filter_type == 'in':
        values_str = ','.join([str(v) for v in value])
        return f"s.{column} IN ({values_str})"
    elif filter_type == 'not in':
        values_str = ','.join([str(v) for v in value])
        return f"s.{column} NOT IN ({values_str})"
    return ""

def _render_query_builder(st_app, data_path: str, stats_path: str, stats_columns: List[str], file_extension: str):
    """Renderizza l'interfaccia di costruzione della query custom."""
    st_app.markdown("---")
    st_app.subheader("🔧 Build Custom Query")
    
    if 'active_filters' not in st_app.session_state:
        st_app.session_state.active_filters = []
    
    st_app.markdown("---")
    st_app.markdown("#### ➕ Aggiungi Filtro")
    
    col1, col2, col3, col4 = st_app.columns([3, 2, 3, 1])
    
    with col1:
        filter_column = st_app.selectbox(
            "Column:",
            options=stats_columns,
        )
    
    with col2:
        filter_type = st_app.selectbox(
            "Operation:",
            options=['>', '<', '>=', '<=', '=', '!=', 'range', 'in', 'not in'],
        )
    
    with col3:
        if filter_type == 'range':
            subcol1, subcol2 = st_app.columns(2)
            with subcol1:
                range_min = st_app.number_input("Min:")
            with subcol2:
                range_max = st_app.number_input("Max:")
            filter_value = [range_min, range_max]
        elif filter_type in ['in', 'not in']:
            filter_value_text = st_app.text_input(
                "Valori (separati da virgola):",
            )
            filter_value = [float(v.strip()) for v in filter_value_text.split(',') if v.strip()]
        else:
            filter_value = st_app.number_input("Valore:")
    
    with col4:
        if st_app.button("➕"):
            new_filter = {
                'column': filter_column,
                'type': filter_type,
                'value': filter_value
            }
            st_app.session_state.active_filters.append(new_filter)
            st_app.rerun()
    
    if st_app.session_state.active_filters:
        st_app.markdown("#### 🎯 Filtri Attivi")
        for idx, f in enumerate(st_app.session_state.active_filters):
            col1, col2 = st_app.columns([5, 1])
            with col1:
                if f['type'] == 'range':
                    st_app.info(f"**{f['column']}** {f['type']}: [{f['value'][0]}, {f['value'][1]}]")
                elif f['type'] in ['in', 'not in']:
                    st_app.info(f"**{f['column']}** {f['type']}: {f['value']}")
                else:
                    st_app.info(f"**{f['column']}** {f['type']} {f['value']}")
            with col2:
                if st_app.button("🗑️", key=f"remove_filter_{idx}"):
                    st_app.session_state.active_filters.pop(idx)
                    st_app.rerun()
    
    # Query SQL custom aggiuntiva
    st_app.markdown("---")
    st_app.markdown("#### 🔍 Condizioni SQL Aggiuntive (Opzionale)")
    custom_query = st_app.text_area(
        "Scrivi condizioni SQL personalizzate:",
        height=100,
        placeholder="Es: d.text_length > 1000 AND s.word_count < 500",
        key="custom_sql_conditions"
    )
    
    # Pulsante per eseguire la query
    st_app.markdown("---")

    st_app.markdown("#### 📝 Nome Nuova Distribution")
    result_folder_name = st_app.text_input(
        "Nome della distribution derivata:",
        help="Questo nome verrà usato per aggiornare il campo _subpath",
        placeholder="Es: filtered_distribution_01",
    )

    if not result_folder_name or not result_folder_name.strip():
        st_app.warning("⚠️ Inserisci un nome valido per la distribution prima di eseguire la query")
    else:
        if st_app.button("▶️ Esegui Query e Visualizza Risultati", use_container_width=True):
            _execute_and_display_query(
                st_app, 
                data_path, 
                stats_path, 
                file_extension,
                st_app.session_state.active_filters,
                custom_query,
                result_folder_name.strip()
            )

def _get_duckdb_reader_function(file_extension: str) -> tuple:
    """Determina la funzione DuckDB appropriata per leggere il file in base all'estensione."""
    ext_lower = file_extension.lower()

    if 'parquet' in ext_lower:
        return 'read_parquet', {}

    if any(x in ext_lower for x in ['.json', '.jsonl']):
        # DuckDB può leggere JSON compressi automaticamente
        return 'read_json', {'format': 'newline_delimited', 'auto_detect': True}

    if '.csv' in ext_lower:
        return 'read_csv', {'auto_detect': True, 'header': True}

    return 'read_json', {'format': 'newline_delimited', 'auto_detect': True}

def _execute_and_display_query(st_app, data_path: str, stats_path: str, 
                               file_extension: str, filters: List[dict], 
                               custom_query: str, folder_name: str):
    """Esegue la query e mostra i risultati."""
    conn = None
    try:
        # Costruisci query con modifica _subpath
        query = _build_visual_query(data_path, file_extension, stats_path, filters, custom_query, folder_name)
        
        st_app.markdown("---")
        st_app.markdown("#### 📝 Query SQL Generata")
        st_app.code(query, language="sql", height=200)

        # Esegui query
        with st_app.spinner("Esecuzione query..."):
            conn = duckdb.connect(':memory:')
            
            logger.info(f"Executing query on {file_extension} files...")
            result_df = conn.execute(query).fetchdf()
            
            conn.close()
            conn = None
        
        # Mostra risultati
        st_app.markdown("---")
        st_app.markdown("#### 📊 Risultati Query")
        st_app.success(f"✅ {len(result_df)} righe trovate")
        
        # Preview dei dati
        st_app.dataframe(result_df.head(100), width='stretch', height=400)
        
        # Salva risultati in session state per il salvataggio
        st_app.session_state.query_result_df = result_df
        st_app.session_state.executed_query = query
        st_app.session_state.result_folder_name = folder_name
        st_app.session_state.show_save_interface = True

    except Exception as e:
        if conn:
            conn.close()
        
        error_msg = str(e)
        st_app.error(f"❌ Errore durante l'esecuzione della query: {error_msg}")
        
        logger.exception(e)
        
        # Mostra query per debug
        with st_app.expander("🔍 Query SQL per Debug"):
            st_app.code(query, language="sql")

def _build_visual_query(data_path: str, file_extension: str, stats_path: str, 
                       filters: List[dict], custom_query: str, folder_name: str = None) -> str:
    """Costruisce la query SQL dai filtri e da opzionali istruzioni SQL User-Defined."""
    stats_pattern = f"'{stats_path}/*.parquet'"
    
    # Determina la funzione di lettura corretta per il formato file
    reader_func, reader_options = _get_duckdb_reader_function(file_extension)
    
    # Costruisci la parte di lettura file con le opzioni appropriate
    if reader_options:
        options_str = ', ' + ', '.join([f"{k}={repr(v)}" for k, v in reader_options.items()])
    else:
        options_str = ''
    
    base_query = f"""
        SELECT d.* 
        FROM {reader_func}('{data_path}/*{file_extension}'{options_str}) AS d
        INNER JOIN read_parquet({stats_pattern}) AS s
        ON d._id_hash = s.id
    """
    
    where_conditions = []
    
    for filter_obj in filters:
        condition = _build_filter_condition(
            filter_obj['column'], 
            filter_obj['type'], 
            filter_obj['value']
        )
        if condition:
            where_conditions.append(condition)
    
    if custom_query and custom_query.strip():
        where_conditions.append(f"({custom_query.strip()})")
    
    if where_conditions:
        base_query += "\nWHERE " + " AND ".join(where_conditions)
    
    # --- MODIFICA _subpath se folder_name è fornito ---
    if folder_name and folder_name.strip():
        final_query = f"""
SELECT 
    t.* EXCLUDE (_subpath),
    split_part(t._subpath, '/', 1) || '/{folder_name.strip()}' AS _subpath
FROM (
    {base_query}
) AS t
"""
        return final_query
    
    return base_query

## QUERY SAVE RESULT HANDLER ##

def _render_save_interface(st, result, repos: Dict):
    """Renderizza l'interfaccia di salvataggio risultati."""
    st.markdown("---")
    st.subheader("💾 Salva Risultati Query")

    folder_name = st.session_state.get('result_folder_name', '')
    distribution_path = st.session_state.current_distribution_path.split("/")
    distribution_path = "/".join(distribution_path[:-1])

    if not folder_name:
        st.warning("⚠️ Nessun nome cartella specificato. Inseriscilo nella sezione query.")
        return

    st.info(f"📁 Cartella di destinazione: **{folder_name}**")
    full_path = f"{distribution_path}/{folder_name}"
    st.write(f"Percorso: `{full_path}`")

    materialize_dataset = st.checkbox("📦 Materializza il dataset (opzionale)")

    col1, col2 = st.columns(2)
    with col1:
        if st.button("💾 Conferma Salvataggio", type="primary", key="confirm_save_opt"):
            _handle_save_confirmation(st, result, Path(full_path), materialize_dataset, repos)
    with col2:
        if st.button("❌ Annulla", key="cancel_save_opt"):
            st.session_state.show_save_interface = False
            st.rerun()

def _handle_save_confirmation(st, result, destination_path: Path, materialize_dataset: bool, repos: Dict):
    """Gestisce la conferma del salvataggio."""
    with st.spinner("Salvataggio in corso..."):
        if materialize_dataset:
            success = _save_query_results(st, result, destination_path, repos)
        else:
            success = _create_query_distribution(st, destination_path, False, repos)

    if success and not materialize_dataset:
        st.session_state.save_success_msg = "✅ Distribuzione acquisita nel sistema"
        st.session_state.save_state = 1
    elif success and materialize_dataset:
        st.session_state.save_success_msg = f"✅ Risultati salvati e distribuzione creata in `{destination_path}`"
        st.session_state.save_state = 1
    else:
        st.session_state.save_error_msg = "❌ Salvataggio fallito. Controlla il log."
        st.session_state.save_state = 2

    if st.button("🔙 Torna ai Risultati"):
        st.rerun()

def _save_query_results(st: Any, result_df, destination_path: Path, repos: Dict) -> bool:
    """Salva i risultati della query in file JSONL.GZ suddivisi per dimensione."""
    TARGET_FILE_SIZE_MB = 120.0
    BYTES_TO_MB = 1024 * 1024
    try:
        if result_df.empty:
            st.info("⚠️ DataFrame vuoto. Nessun file da salvare.")
            return _create_query_distribution(st, destination_path, False, repos)

        if destination_path.exists() and any(destination_path.iterdir()):
            st.error(f"❌ '{destination_path}' esiste già e non è vuota!")
            logger.error(f"[_save_query_results] Cartella esiste già: {destination_path}")
            return False

        destination_path.mkdir(parents=True, exist_ok=True)
        st.info(f"📁 Creata nuova cartella: {destination_path}")

        total_size_mb = result_df.memory_usage(deep=True).sum() / BYTES_TO_MB
        ratio = total_size_mb / TARGET_FILE_SIZE_MB
        num_chunks = max(1, int(np.ceil(ratio)) if ratio % 1 > 0.5 else int(np.floor(ratio)))
        num_records = len(result_df)
        chunk_size = int(np.ceil(num_records / num_chunks))
        logger.info(f"[_save_query_results] {total_size_mb:.2f} MB → {num_chunks} chunk, size={chunk_size}")

        progress_bar = st.progress(0)
        status_text = st.empty()

        for i in range(num_chunks):
            start_idx = i * chunk_size
            end_idx = min(start_idx + chunk_size, num_records)
            if start_idx >= num_records:
                break
            file_name = f"query_results_{i+1:05d}.jsonl.gz"
            chunk_df = result_df.iloc[start_idx:end_idx].assign(_filename=file_name)
            status_text.text(f"Salvataggio file {i+1}/{num_chunks} ({len(chunk_df)} record)...")
            chunk_df.to_json(destination_path / file_name, orient='records', lines=True,
                             force_ascii=False, compression='gzip')
            del chunk_df
            progress_bar.progress((i + 1) / num_chunks)

        progress_bar.progress(1.0)
        status_text.empty()
        files_created = list(destination_path.glob("*.jsonl.gz"))
        st.success(f"✅ Salvataggio completato in {len(files_created)} file!")
        st.info(f"📊 Totale: {num_records} record(s) in {len(files_created)} file(s)")

        return _create_query_distribution(st, destination_path, True, repos)

    except Exception as e:
        st.error(f"❌ Errore nel salvataggio: {str(e)}")
        logger.error(f"[_save_query_results] Errore generale: {str(e)}")
        return False

def _create_query_distribution(st, destination_path: Path, materialize: bool, repos: Dict) -> bool:
    """Crea una nuova distribuzione, gestendo la creazione di un nuovo dataset se necessario.""" # <--- MODIFICA: Aggiornata docstring
    try:
        from datetime import datetime, timezone
        import copy 

        current_dist = st.session_state.current_distribution
        old_dataset = repos['dataset'].get_by_id(current_dist.dataset_id)

        if not old_dataset:
            st.error("❌ Dataset originale non trovato nel database.")
            return False

        # 1. Prepariamo l'URI della nuova distribuzione
        new_dist_uri = f"{BASE_PREFIX}{to_binded_path(str(destination_path))}"

        # 2. <--- MODIFICA: Logica Critica - Verifica appartenenza gerarchica
        # Se l'URI del dataset è uguale all'URI della nuova distribuzione
        if old_dataset.uri == current_dist.uri or not new_dist_uri.startswith(old_dataset.uri):
            logger.info("⚠️ Nuova distribuzione fuori dal path del dataset originale. Creazione nuovo Dataset.")

            # Creiamo il nuovo dataset ereditando i campi dal vecchio dataset
            new_dataset = Dataset(
                id=None,  # Sarà generato dal DB
                uri=new_dist_uri,  # Il nuovo dataset ha come radice l'uri della distribuzione
                name=f"dataset_{destination_path.name}",
                languages=copy.deepcopy(old_dataset.languages),
                derived_card=old_dataset.derived_card,
                derived_dataset=old_dataset.id,  # Punta al dataset genitore
                dataset_type=old_dataset.dataset_type,
                globs=['*.jsonl.gz'] if materialize else [],  # default a jsonl.gz
                description=f"Dataset derivato da query su {old_dataset.name}",
                source=old_dataset.source,
                version=old_dataset.version,
                license=old_dataset.license,
                step=3,
                issued=datetime.now(timezone.utc),
                modified=datetime.now(timezone.utc)
            )

            new_dataset_obj = repos['dataset'].insert(new_dataset)
            if not new_dataset_obj:
                st.error("❌ Impossibile creare il nuovo Dataset nel database.")
                logger.error("[_create_query_distribution] Insert nuovo Dataset fallito.")
                return False
            target_dataset_id = new_dataset_obj.id
            logger.info(f"[_create_query_distribution] Nuovo Dataset creato con ID: {target_dataset_id}")
        else:
            # Caso standard: la distribuzione è una sottocartella del dataset esistente
            target_dataset_id = old_dataset.id
            logger.info("✅ Nuova distribuzione all'interno del dataset esistente.")

        # 3. Creazione della Nuova Distribuzione (puntando al target_dataset_id corretto)
        tags = current_dist.tags or []
        if not isinstance(tags, list):
            tags = list(tags) if tags else []

        new_distribution = Distribution(
            id=None, 
            uri=new_dist_uri,
            tokenized_uri=None,
            dataset_id=target_dataset_id, 
            glob='*.jsonl.gz',
            format='.jsonl.gz',
            query=_compact_sql_query(st.session_state.get('executed_query', '')),
            derived_from=current_dist.id,
            split=current_dist.split,
            src_schema=None, 
            name=f"query__{current_dist.name}",
            description=f"Risultati query eseguita il {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M')} su {current_dist.name} dataset.",
            lang=current_dist.lang,
            tags=tags + ["query"],
            license=current_dist.license,
            version=current_dist.version,
            issued=datetime.now(timezone.utc),
            modified=datetime.now(timezone.utc),
            materialized=materialize,
            step=3
        )

        result = repos['distribution'].insert(new_distribution)

        if not result:
            st.error("❌ Errore nella creazione della distribution nel database.")
            logger.error("[_create_query_distribution] Insert distribution fallito.")
            return False

        if materialize:
            target_ds = repos['dataset'].get_by_id(target_dataset_id)
            if target_ds:
                target_ds.globs = generate_filtered_globs(target_ds.uri)
                repos['dataset'].update(target_ds)
                logger.info(f"[_create_query_distribution] Globs dataset {target_dataset_id} aggiornati.")
            else:
                logger.warning(
                    f"[_create_query_distribution] Dataset {target_dataset_id} "
                    "non trovato per aggiornamento globs."
                )

        st.success(f"✅ Nuova distribution creata con ID: {result.id}")
        logger.info(f"[_create_query_distribution] Distribution creata con ID: {result.id}")
        return True

    except Exception as e:
        st.error(f"❌ Errore nella creazione distribuzione/dataset: {str(e)}") # <--- MODIFICA: Messaggio aggiornato
        logger.error(f"Errore creazione distribution: {e}")
        logger.error(traceback.format_exc())
        return False

## RENDERING FUNCTIONS ###

def _render_navigation_header(st_app):
    """Renderizza l'header con navigazione."""
    st_app.header("🔍 Query Dataset con Filtri")
    st_app.write("Interroga il tuo dataset con filtri SQL, utilizzando anche le statistiche come discriminante.")

    if st_app.button("🏠 Torna alla Distribution"):
        reset_dashboard_session_state(st_app, KEYS_TO_KEEP)
        st_app.session_state.current_stage = "mapped_distribution_main"
        st_app.rerun()

def _render_mode_selector(st_app):
    """Renderizza il selettore della modalità di query."""
    st_app.markdown("---")
    st_app.subheader("🎯 Modalità di Interrogazione")
    
    col1, col2 = st_app.columns(2)
    
    with col1:
        if st_app.button("📊 Visualizza Statistiche", width='stretch'):
            st_app.session_state.query_mode = "visualize_stats"
            st_app.rerun()
    
    with col2:
        if st_app.button("🔧 Build Custom Query", width='stretch'):
            st_app.session_state.query_mode = "build_query"
            st_app.rerun()

### MAIN HANDLER ###

def show_mapped_distribution_low_level_stats_query_handler(st_app):
    """Main handler per la pagina di query con statistiche."""
    try:
        # Inizializza lo stato di salvataggio se non esiste
        if 'show_save_interface' not in st_app.session_state:
            st_app.session_state.show_save_interface = False
            
        distribution = st_app.session_state.current_distribution
        
        _render_navigation_header(st_app)
        data_path, stats_path = _get_distribution_paths(distribution)
        file_extension = _get_file_extension(data_path)
        
        if not os.path.exists(stats_path):
            st_app.error(f"❌ Path statistiche non trovato: {stats_path}")
            return
        
        # Estrai colonne stats
        stats_columns = _extract_stats_columns(stats_path)
        
        if not stats_columns:
            st_app.warning("⚠️ Nessuna colonna di statistiche trovata")
            return
        
        # Info distribution
        st_app.info(f"📦 Distribution: **{distribution.name}** | 📁 {len(stats_columns)} statistiche disponibili")
        
        # Inizializza query_mode se non esiste
        if 'query_mode' not in st_app.session_state:
            st_app.session_state.query_mode = None

        _render_mode_selector(st_app)
        
        if st_app.session_state.query_mode == "visualize_stats":
            _render_stats_visualization(st_app, stats_path, stats_columns)
        elif st_app.session_state.query_mode == "build_query":
            _render_query_builder(st_app, data_path, stats_path, stats_columns, file_extension)
        
        if st_app.session_state.get('show_save_interface', False):

            # Verifica che abbiamo i risultati della query
            if 'query_result_df' in st_app.session_state:
                repos = _initialize_repositories(st_app)
                _render_save_interface(st_app, st_app.session_state.query_result_df, repos)
            else:
                st_app.warning("⚠️ Nessun risultato query trovato per il salvataggio")
                st_app.session_state.show_save_interface = False
        
    except Exception as e:
        st_app.error(f"❌ Errore: {str(e)}")
        logger.exception(e)