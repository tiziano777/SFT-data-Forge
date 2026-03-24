from dataclasses import dataclass, field
from typing import Optional

@dataclass
class DownloadConfig:
    source: str  # 'url' o 'huggingface'
    repo_id: Optional[str] = None
    url: Optional[str] = None
    file_path: Optional[str] = None # Utilizzato se download_type è 'file specifico'
    download_type: str = 'dataset completo'
    max_workers: int = 4 # Default ragionevole per download paralleli
    
    def __post_init__(self):
        """Validazione minima post-istanziazione"""
        if self.source == 'huggingface' and not self.repo_id:
            raise ValueError("repo_id è obbligatorio per sorgente Hugging Face")
        if self.source == 'url' and not self.url:
            raise ValueError("url è obbligatorio per sorgente URL")

@dataclass
class DownloadResult:
    success: bool
    message: str
    dataset_id: Optional[str] = None
    file_path: Optional[str] = None
    file_size: Optional[int] = None
    shell_command: Optional[str] = None # Il prototipo CLI generato
    error_details: Optional[str] = None # Utile per il debugging in UI in caso di success=False