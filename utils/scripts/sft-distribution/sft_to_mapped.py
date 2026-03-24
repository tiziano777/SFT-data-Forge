import pandas as pd
import os
import re

def custom_normalize(text):
    """
    1. Lowercase
    2. Spaces -> '__' (double underscore)
    3. Keeps: alphanumerics, underscores (_), hyphens (-) and dots (.)
    """
    if not isinstance(text, str) or pd.isna(text):
        return ""

    # 1. Lowercase and strip
    s = text.lower().strip()

    # 2. Replace spaces with double underscore
    s = re.sub(r'\s+', '__', s)

    # 3. Regex: keep a-z, 0-9, underscore, hyphen and dot
    # Note: dot and hyphen are escaped with \
    s = re.sub(r'[^a-z0-9_\-\.]', '', s)

    # Final cleanup of any remaining symbols at edges
    return s.strip('_').strip('-').strip('.')

def process_single_parquet(input_path, output_dir):
    if not os.path.exists(input_path):
        print(f"Error: File does not exist at path {input_path}")
        return

    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    print(f"Processing: {input_path}...")

    # 1. Read
    df = pd.read_parquet(input_path)

    # 2. Handle LANG (ISO-2 and default 'un')
    if 'LANG' in df.columns:
        df['lang'] = df['LANG'].astype(str).str[:2].str.lower()
        df['lang'] = df['lang'].replace(['na', 'no', 'none', 'nan'], 'un').fillna('un')
    else:
        df['lang'] = 'un'

    # 3. Handle quality (Numeric + default 1)
    if 'QUALITY SCORE' in df.columns:
        df['quality'] = pd.to_numeric(df['QUALITY SCORE'], errors='coerce').fillna(1).astype(int)
    else:
        df['quality'] = 1

    # 4. Rename and cleanup URI (remove rows without uri)
    df = df.rename(columns={'DIR INPUT': 'uri'})
    df = df.dropna(subset=['uri'])
    df = df[df['uri'].astype(str).str.strip() != '']
    
    # 5. Dataset ID base
    df['dataset_id_base'] = df['dataset_id'].fillna(df['NAME'])

    # 6. DEDUPLICATION AND URI SUFFIX LOGIC
    df = df.drop_duplicates(subset=['dataset_id_base', 'uri'], keep='first')
    df['counts'] = df.groupby('dataset_id_base')['dataset_id_base'].transform('count')
    
    def generate_names(row):
        base_id = str(row['dataset_id_base'])
        lang = str(row['lang'])
        name_val = str(row['NAME'])
        uri_val = str(row['uri'])
        
        # Extract the last path element (namespace)
        namespace = uri_val.rstrip('/').split('/')[-1]
        
        if row['counts'] > 1:
            # If ID is duplicated, add the URI suffix
            raw_name = f"{lang} {name_val} {namespace}"
            raw_id = f"{lang} {base_id} {namespace}"
        else:
            # If unique
            raw_name = f"{lang} {name_val}"
            raw_id = f"{lang} {base_id}"
            
        # Apply custom normalization (lowercase and double underscore)
        return pd.Series([custom_normalize(raw_name), custom_normalize(raw_id)])

    # Apply the logic row by row
    df[['name', 'dataset_id']] = df.apply(generate_names, axis=1)

    # 7. Finalization
    df['dataset'] = df['name']
    df['step'] = 3
    
    cols_order = ['name', 'quality', 'dataset_id', 'dataset', 'uri', 'step', 'lang']
    df_final = df[cols_order].copy()

    # 8. Write output
    file_name = os.path.basename(input_path)
    output_path = os.path.join(output_dir, f"mapped_{file_name}")
    df_final.to_parquet(output_path, engine='pyarrow', index=False)
    
    print(f"--- Final Report ---")
    print(f"Rows saved: {len(df_final)}")
    
    dupes_view = df_final[df['counts'] > 1]
    if not dupes_view.empty:
        print("\nExamples of differentiated and normalized IDs:")
        print(dupes_view[['name', 'dataset_id']].head(5))
    
    return df_final

# --- CONFIGURATION ---
from pathlib import Path
_SCRIPTS_DIR = Path(__file__).resolve().parents[1]
FILE_INPUT = str(_SCRIPTS_DIR / "velvet___b_expj_datasets_finetuning.parquet")
DIR_OUTPUT = str(Path(__file__).resolve().parent)

if __name__ == "__main__":
    process_single_parquet(FILE_INPUT, DIR_OUTPUT)