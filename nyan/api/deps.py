import os
import threading
from typing import Generator

from pymongo import MongoClient
from pymongo.collection import Collection

from nyan.mongo import read_config

# Config paths are read at import time; container env changes require a restart.
MONGO_CONFIG_PATH = os.environ.get("MONGO_CONFIG_PATH", "configs/mongo_config.json")
CHANNELS_INFO_PATH = os.environ.get("CHANNELS_INFO_PATH", "channels.json")

# Module-level MongoClient — created once and reused across requests.
# MongoClient is thread-safe and manages an internal connection pool.
_mongo_client: MongoClient = None  # type: ignore[assignment]
_mongo_client_lock = threading.Lock()


def _get_mongo_client() -> MongoClient:  # type: ignore[type-arg]
    global _mongo_client
    if _mongo_client is None:
        with _mongo_client_lock:
            if _mongo_client is None:
                config = read_config(MONGO_CONFIG_PATH)
                _mongo_client = MongoClient(**config["client"])
    return _mongo_client


def _get_database():  # type: ignore[no-untyped-def]
    config = read_config(MONGO_CONFIG_PATH)
    return _get_mongo_client()[config["database_name"]]


def get_mongo_config_path() -> str:
    return MONGO_CONFIG_PATH


def get_channels_path() -> str:
    return CHANNELS_INFO_PATH


def get_documents_col() -> Generator[Collection, None, None]:  # type: ignore[type-arg]
    config = read_config(MONGO_CONFIG_PATH)
    db = _get_mongo_client()[config["database_name"]]
    yield db[config["documents_collection_name"]]


def get_clusters_col() -> Generator[Collection, None, None]:  # type: ignore[type-arg]
    config = read_config(MONGO_CONFIG_PATH)
    db = _get_mongo_client()[config["database_name"]]
    yield db[config["clusters_collection_name"]]


def get_annotated_documents_col() -> Generator[Collection, None, None]:  # type: ignore[type-arg]
    config = read_config(MONGO_CONFIG_PATH)
    db = _get_mongo_client()[config["database_name"]]
    yield db[config["annotated_documents_collection_name"]]


def get_topics_col() -> Generator[Collection, None, None]:  # type: ignore[type-arg]
    config = read_config(MONGO_CONFIG_PATH)
    db = _get_mongo_client()[config["database_name"]]
    yield db[config.get("topics_collection_name", "topics")]
