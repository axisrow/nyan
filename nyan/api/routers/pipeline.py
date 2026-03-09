import os
import subprocess
import threading
from datetime import datetime
from typing import Any, Dict, Optional

from fastapi import APIRouter, BackgroundTasks, Depends, HTTPException, Query
from pymongo.collection import Collection

from nyan.api.deps import get_clusters_col, get_documents_col, get_mongo_config_path
from nyan.api.schemas import PipelineStatusSchema

router = APIRouter(prefix="/pipeline", tags=["pipeline"])

_daemon_state: Dict[str, Any] = {
    "running": False,
    "last_run": None,
    "error": None,
}
_daemon_lock = threading.Lock()

_CLIENT_CONFIG_PATH = os.environ.get("CLIENT_CONFIG_PATH", "configs/client_config.json")
_ANNOTATOR_CONFIG_PATH = os.environ.get("ANNOTATOR_CONFIG_PATH", "configs/annotator_config.json")
_CLUSTERER_CONFIG_PATH = os.environ.get("CLUSTERER_CONFIG_PATH", "configs/clusterer_config.json")
_RANKER_CONFIG_PATH = os.environ.get("RANKER_CONFIG_PATH", "configs/ranker_config.json")
_RENDERER_CONFIG_PATH = os.environ.get("RENDERER_CONFIG_PATH", "configs/renderer_config.json")
_DAEMON_CONFIG_PATH = os.environ.get("DAEMON_CONFIG_PATH", "configs/daemon_config.json")
_PIPELINE_CHANNELS_INFO_PATH = os.environ.get("CHANNELS_INFO_PATH", "channels.json")

_CRAWL_CHANNELS_FILE = _PIPELINE_CHANNELS_INFO_PATH
_CRAWL_FETCH_TIMES = os.environ.get("FETCH_TIMES_PATH", "crawler/fetch_times.json")


def _run_daemon_iteration(
    mongo_config_path: str,
    client_config_path: str,
    annotator_config_path: str,
    clusterer_config_path: str,
    ranker_config_path: str,
    renderer_config_path: str,
    daemon_config_path: str,
    channels_info_path: str,
) -> None:
    from nyan.daemon import Daemon

    error: Optional[str] = None
    try:
        daemon = Daemon(
            client_config_path=client_config_path,
            annotator_config_path=annotator_config_path,
            clusterer_config_path=clusterer_config_path,
            ranker_config_path=ranker_config_path,
            channels_info_path=channels_info_path,
            renderer_config_path=renderer_config_path,
            daemon_config_path=daemon_config_path,
        )
        daemon(
            input_path=None,
            mongo_config_path=mongo_config_path,
            posted_clusters_path=None,
        )
    except Exception as e:
        error = str(e)
    finally:
        with _daemon_lock:
            _daemon_state["running"] = False
            _daemon_state["last_run"] = datetime.utcnow().isoformat()
            _daemon_state["error"] = error


@router.post("/daemon")
def run_daemon(
    background_tasks: BackgroundTasks,
    mongo_config_path: str = Depends(get_mongo_config_path),
) -> Dict[str, Any]:
    """Start a single daemon iteration in the background.

    Config paths are read from environment variables (CLIENT_CONFIG_PATH,
    ANNOTATOR_CONFIG_PATH, etc.) or fall back to defaults under configs/.
    This endpoint only works correctly with a single-worker deployment.
    """
    with _daemon_lock:
        if _daemon_state["running"]:
            raise HTTPException(status_code=409, detail="Daemon iteration already running")
        _daemon_state["running"] = True

    background_tasks.add_task(
        _run_daemon_iteration,
        mongo_config_path=mongo_config_path,
        client_config_path=_CLIENT_CONFIG_PATH,
        annotator_config_path=_ANNOTATOR_CONFIG_PATH,
        clusterer_config_path=_CLUSTERER_CONFIG_PATH,
        ranker_config_path=_RANKER_CONFIG_PATH,
        renderer_config_path=_RENDERER_CONFIG_PATH,
        daemon_config_path=_DAEMON_CONFIG_PATH,
        channels_info_path=_PIPELINE_CHANNELS_INFO_PATH,
    )
    return {"status": "started"}


_crawl_proc: Optional[subprocess.Popen] = None  # type: ignore[type-arg]
_crawl_lock = threading.Lock()


@router.post("/crawl")
def run_crawl(
    hours: int = Query(default=24, ge=1, le=168),
) -> Dict[str, str]:
    """Start the Scrapy crawler in a subprocess.

    File paths are read from environment variables (CHANNELS_INFO_PATH,
    FETCH_TIMES_PATH) or fall back to defaults.
    This endpoint only works correctly with a single-worker deployment.
    """
    global _crawl_proc
    with _crawl_lock:
        if _crawl_proc and _crawl_proc.poll() is None:
            raise HTTPException(status_code=409, detail="Crawl already running")
        _crawl_proc = subprocess.Popen(
            [
                "scrapy",
                "crawl",
                "telegram",
                "-a",
                f"channels_file={_CRAWL_CHANNELS_FILE}",
                "-a",
                f"fetch_times={_CRAWL_FETCH_TIMES}",
                "-a",
                f"hours={hours}",
            ]
        )
        pid = _crawl_proc.pid
    return {"status": "started", "pid": str(pid)}


@router.get("/status", response_model=PipelineStatusSchema)
def pipeline_status(
    documents_col: Collection = Depends(get_documents_col),  # type: ignore[type-arg]
    clusters_col: Collection = Depends(get_clusters_col),  # type: ignore[type-arg]
    mongo_config_path: str = Depends(get_mongo_config_path),
) -> PipelineStatusSchema:
    with _daemon_lock:
        daemon_running = _daemon_state["running"]
        daemon_last_run = _daemon_state["last_run"]
        daemon_last_error = _daemon_state["error"]
    return PipelineStatusSchema(
        documents_count=documents_col.estimated_document_count(),
        clusters_count=clusters_col.estimated_document_count(),
        mongo_config_path=mongo_config_path,
        daemon_running=daemon_running,
        daemon_last_run=daemon_last_run,
        daemon_last_error=daemon_last_error,
    )
