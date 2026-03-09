import os
import threading
from typing import Any, Dict, Generator, Optional

from pymongo import MongoClient
from pymongo.collection import Collection

from nyan.mongo import read_config

# Config paths are read at import time; container env changes require a restart.
MONGO_CONFIG_PATH = os.environ.get("MONGO_CONFIG_PATH", "configs/mongo_config.json")
CHANNELS_INFO_PATH = os.environ.get("CHANNELS_INFO_PATH", "channels.json")

# Module-level singletons — created once and reused across requests.
# MongoClient is thread-safe and manages an internal connection pool.
_mongo_client: Optional[MongoClient] = None  # type: ignore[type-arg]
_mongo_config: Optional[Dict[str, Any]] = None
_mongo_init_lock = threading.Lock()


def _get_config() -> Dict[str, Any]:
    global _mongo_config
    if _mongo_config is None:
        with _mongo_init_lock:
            if _mongo_config is None:
                _mongo_config = read_config(MONGO_CONFIG_PATH)
    return _mongo_config


def _get_mongo_client() -> MongoClient:  # type: ignore[type-arg]
    global _mongo_client
    if _mongo_client is None:
        with _mongo_init_lock:
            if _mongo_client is None:
                config = _get_config()
                _mongo_client = MongoClient(**config["client"])
    return _mongo_client


def get_mongo_config_path() -> str:
    return MONGO_CONFIG_PATH


def get_channels_path() -> str:
    return CHANNELS_INFO_PATH


def get_documents_col() -> Generator[Collection, None, None]:  # type: ignore[type-arg]
    config = _get_config()
    db = _get_mongo_client()[config["database_name"]]
    yield db[config["documents_collection_name"]]


def get_clusters_col() -> Generator[Collection, None, None]:  # type: ignore[type-arg]
    config = _get_config()
    db = _get_mongo_client()[config["database_name"]]
    yield db[config["clusters_collection_name"]]
