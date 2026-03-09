import subprocess
import threading
from datetime import datetime
from typing import Any, Dict, Optional

from fastapi import APIRouter, BackgroundTasks, Depends, HTTPException
from pymongo.collection import Collection

from nyan.api.deps import get_clusters_col, get_documents_col, get_mongo_config_path
from nyan.api.schemas import PipelineStatusSchema

router = APIRouter(prefix="/pipeline", tags=["pipeline"])

_daemon_state: Dict[str, Any] = {
    "running": False,
    "last_run": None,
    "error": None,
}


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

    _daemon_state["running"] = True
    _daemon_state["error"] = None
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
        _daemon_state["error"] = str(e)
    finally:
        _daemon_state["running"] = False
        _daemon_state["last_run"] = datetime.utcnow().isoformat()


@router.post("/daemon")
def run_daemon(
    background_tasks: BackgroundTasks,
    mongo_config_path: str = Depends(get_mongo_config_path),
    client_config_path: str = "configs/client_config.json",
    annotator_config_path: str = "configs/annotator_config.json",
    clusterer_config_path: str = "configs/clusterer_config.json",
    ranker_config_path: str = "configs/ranker_config.json",
    renderer_config_path: str = "configs/renderer_config.json",
    daemon_config_path: str = "configs/daemon_config.json",
    channels_info_path: str = "channels.json",
) -> Dict[str, Any]:
    if _daemon_state["running"]:
        raise HTTPException(status_code=409, detail="Daemon iteration already running")

    background_tasks.add_task(
        _run_daemon_iteration,
        mongo_config_path=mongo_config_path,
        client_config_path=client_config_path,
        annotator_config_path=annotator_config_path,
        clusterer_config_path=clusterer_config_path,
        ranker_config_path=ranker_config_path,
        renderer_config_path=renderer_config_path,
        daemon_config_path=daemon_config_path,
        channels_info_path=channels_info_path,
    )
    return {"status": "started"}


_crawl_proc: Optional[subprocess.Popen] = None  # type: ignore[type-arg]
_crawl_lock = threading.Lock()


@router.post("/crawl")
def run_crawl(
    channels_file: str = "channels.json",
    fetch_times: str = "crawler/fetch_times.json",
    hours: int = 24,
) -> Dict[str, str]:
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
                f"channels_file={channels_file}",
                "-a",
                f"fetch_times={fetch_times}",
                "-a",
                f"hours={hours}",
            ]
        )
    return {"status": "started", "pid": str(_crawl_proc.pid)}


@router.get("/status", response_model=PipelineStatusSchema)
def pipeline_status(
    documents_col: Collection = Depends(get_documents_col),  # type: ignore[type-arg]
    clusters_col: Collection = Depends(get_clusters_col),  # type: ignore[type-arg]
    mongo_config_path: str = Depends(get_mongo_config_path),
) -> PipelineStatusSchema:
    return PipelineStatusSchema(
        documents_count=documents_col.estimated_document_count(),
        clusters_count=clusters_col.estimated_document_count(),
        mongo_config_path=mongo_config_path,
        daemon_running=_daemon_state["running"],
        daemon_last_run=_daemon_state["last_run"],
        daemon_last_error=_daemon_state["error"],
    )
