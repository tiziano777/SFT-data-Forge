import sys
import os
import time
import fnmatch
from concurrent.futures import ThreadPoolExecutor
from huggingface_hub import list_repo_files, hf_hub_download

# Massimizza la velocità di download per singolo file tramite core in Rust
os.environ["HF_HUB_ENABLE_HF_TRANSFER"] = "1"

def download_file_with_retry(repo_id, filename, local_dir, token, retries=10):
    """Scarica un singolo file con backoff esponenziale e resume attivo."""
    for i in range(retries):
        try:
            hf_hub_download(
                repo_id=repo_id,
                filename=filename,
                local_dir=local_dir,
                repo_type="dataset",
                token=token,
                local_dir_use_symlinks=False,
                resume_download=True
            )
            return True, filename
        except Exception as e:
            if i < retries - 1:
                wait = (2 ** i) + 5
                print(f"⚠️ Errore su {filename}: {e}. Retry in {wait}s... ({i+1}/{retries})")
                time.sleep(wait)
            else:
                return False, filename

def run_standalone_download(repo_id, output_base, token, max_workers, log_file):
    # 1. Salvataggio del PID per monitoraggio manuale (come richiesto dai colleghi)
    os.makedirs(output_base, exist_ok=True)
    pid_file = os.path.join(output_base, "download.pid")
    with open(pid_file, "w") as f:
        f.write(str(os.getpid()))

    # 2. Definizione Exclude List (file di progetto/meta-data inutili)
    EXCLUDE_PATTERNS = [".git*", ".gitignore", "README.md", "LICENSE*", ".huggingface*"]

    try:
        print(f"🚀 Avvio scansione repository: {repo_id}")
        all_files = list_repo_files(repo_id, repo_type="dataset", token=token)
        
        # 3. Applicazione Filtri Exclude (Logica fnmatch per supportare i glob)
        data_files = []
        for f in all_files:
            if any(fnmatch.fnmatch(f, pattern) for pattern in EXCLUDE_PATTERNS):
                print(f"🚫 Escluso: {f}")
                continue
            data_files.append(f)

        print(f"📦 File totali da scaricare: {len(data_files)} (Workers: {max_workers})")

        # 4. Esecuzione Parallela
        with ThreadPoolExecutor(max_workers=int(max_workers)) as executor:
            futures = [
                executor.submit(download_file_with_retry, repo_id, f, output_base, token) 
                for f in data_files
            ]
            
            for future in futures:
                success, name = future.result()
                if success:
                    print(f"✅ COMPLETATO: {name}")
                else:
                    print(f"❌ FALLITO DEFINITIVAMENTE: {name}")

        print(f"🏁 Processo terminato per {repo_id}")
        if os.path.exists(pid_file):
            os.remove(pid_file)

    except Exception as e:
        print(f"💥 ERRORE CRITICO: {e}")

if __name__ == "__main__":
    # Argomenti: repo_id, output_base, token, max_workers, log_file
    if len(sys.argv) >= 5:
        r_id = sys.argv[1]
        out = sys.argv[2]
        tok = sys.argv[3] if sys.argv[3].lower() != "none" and sys.argv[3] != "" else None
        workers = sys.argv[4]
        log = sys.argv[5] if len(sys.argv) > 5 else None
        
        run_standalone_download(r_id, out, tok, workers, log)