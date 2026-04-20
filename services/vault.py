"""
Simple secret store backed by config/secrets.json.
Environment variables take precedence (e.g. ANTHROPIC_API_KEY overrides the file).
"""
import json
import os
from typing import Optional


class Vault:
    def __init__(self, secrets_path: str = "config/secrets.json"):
        try:
            with open(secrets_path) as f:
                self._secrets: dict = json.load(f)
        except FileNotFoundError:
            self._secrets = {}

    def get(self, key: str) -> Optional[str]:
        # Env vars win (uppercase, e.g. ANTHROPIC_API_KEY)
        env_val = os.environ.get(key.upper())
        if env_val:
            return env_val
        val = self._secrets.get(key)
        # Don't hand back placeholder strings (only applicable to str values)
        if isinstance(val, str) and val.startswith("YOUR_"):
            return None
        return val
