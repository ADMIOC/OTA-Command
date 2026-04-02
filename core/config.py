"""
OTA Command — Configuration Loader
Loads settings.yaml + environment secrets into a unified config object.
"""

import os
import yaml
from pathlib import Path

_ROOT = Path(__file__).resolve().parent.parent
_CONFIG_CACHE = None


def load_config(config_path: str = None) -> dict:
    """Load and merge YAML config with environment variables."""
    global _CONFIG_CACHE
    if _CONFIG_CACHE is not None:
        return _CONFIG_CACHE

    if config_path is None:
        config_path = _ROOT / "config" / "settings.yaml"

    with open(config_path, "r") as f:
        cfg = yaml.safe_load(f)

    # Overlay environment secrets
    cfg["secrets"] = {
        "github_token": os.getenv("GITHUB_TOKEN", ""),
        "anthropic_api_key": os.getenv("ANTHROPIC_API_KEY", ""),
        "youtube_api_key": os.getenv("YOUTUBE_API_KEY", ""),
        "elevenlabs_api_key": os.getenv("ELEVENLABS_API_KEY", ""),
        "descript_api_key": os.getenv("DESCRIPT_API_KEY", ""),
        "restream_api_key": os.getenv("RESTREAM_API_KEY", ""),
        "notion_api_key": os.getenv("NOTION_API_KEY", ""),
        "google_service_account": os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON", ""),
    }

    _CONFIG_CACHE = cfg
    return cfg


def get_secret(key: str) -> str:
    """Get a specific secret by key."""
    cfg = load_config()
    value = cfg["secrets"].get(key, "")
    if not value:
        raise ValueError(f"Secret '{key}' is not set. Check secrets.env or GitHub repo secrets.")
    return value


def get_setting(*keys, default=None):
    """Traverse nested config keys. e.g. get_setting('discovery', 'poll_interval_minutes')"""
    cfg = load_config()
    node = cfg
    for k in keys:
        if isinstance(node, dict) and k in node:
            node = node[k]
        else:
            return default
    return node
