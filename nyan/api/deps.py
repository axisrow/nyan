import os
from typing import Generator

from pymongo.collection import Collection

from nyan.mongo import (
    get_documents_collection,
    get_clusters_collection,
    get_annotated_documents_collection,
    get_topics_collection,
)

MONGO_CONFIG_PATH = os.environ.get("MONGO_CONFIG_PATH", "configs/mongo_config.json")
CHANNELS_INFO_PATH = os.environ.get("CHANNELS_INFO_PATH", "channels.json")


def get_mongo_config_path() -> str:
    return MONGO_CONFIG_PATH


def get_channels_path() -> str:
    return CHANNELS_INFO_PATH


def get_documents_col() -> Generator[Collection, None, None]:  # type: ignore[type-arg]
    col = get_documents_collection(MONGO_CONFIG_PATH)
    try:
        yield col
    finally:
        col.database.client.close()


def get_clusters_col() -> Generator[Collection, None, None]:  # type: ignore[type-arg]
    col = get_clusters_collection(MONGO_CONFIG_PATH)
    try:
        yield col
    finally:
        col.database.client.close()


def get_annotated_documents_col() -> Generator[Collection, None, None]:  # type: ignore[type-arg]
    col = get_annotated_documents_collection(MONGO_CONFIG_PATH)
    try:
        yield col
    finally:
        col.database.client.close()


def get_topics_col() -> Generator[Collection, None, None]:  # type: ignore[type-arg]
    col = get_topics_collection(MONGO_CONFIG_PATH)
    try:
        yield col
    finally:
        col.database.client.close()
