import os
from pathlib import Path

_SECRETS_FILE = Path(__file__).parent / ".secrets"


def get_secret(key: str) -> str:
    val = os.environ.get(key)
    if val is not None:
        return val
    if _SECRETS_FILE.exists():
        for line in _SECRETS_FILE.read_text().splitlines():
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            if "=" in line:
                k, v = line.split("=", 1)
                if k.strip() == key:
                    return v.strip()
    return ""
