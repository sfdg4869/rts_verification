from __future__ import annotations

from functools import lru_cache
from pathlib import Path
import subprocess


_DGSERVER_JAR_PATH = Path(__file__).resolve().parents[2] / "DGServer.jar"


def _looks_encrypted(value: str) -> bool:
    text = str(value or "").strip()
    return bool(text) and text.startswith("_")


@lru_cache(maxsize=512)
def decrypt_dg_password(value: str) -> str:
    text = str(value or "").strip()
    if not text:
        return ""
    if not _looks_encrypted(text):
        return text
    if not _DGSERVER_JAR_PATH.exists():
        return text

    try:
        result = subprocess.run(
            ["java", "-jar", str(_DGSERVER_JAR_PATH), "decrypt", text],
            capture_output=True,
            text=True,
            timeout=10,
            check=False,
        )
    except Exception:
        return text

    if result.returncode != 0:
        return text

    decrypted = (result.stdout or "").strip()
    return decrypted or text
