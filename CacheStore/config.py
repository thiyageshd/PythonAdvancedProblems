import os

def get_from_env(key: str, fallback: str) -> str:
    if key and key in os.environ:
        return os.environ[key]
    return fallback