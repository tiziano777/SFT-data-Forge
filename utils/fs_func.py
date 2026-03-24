import os

def is_dir(path):
    return os.path.isdir(path)

def list_dirs(path):
    try:
        return [d for d in os.listdir(path) if is_dir(os.path.join(path, d))]
    except FileNotFoundError:
        return []

def list_files(path):
    try:
        return [f for f in os.listdir(path) if os.path.isfile(os.path.join(path, f))]
    except FileNotFoundError as e:
        print(f"Directory not found: {e}")
        return []
