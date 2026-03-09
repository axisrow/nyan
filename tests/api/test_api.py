"""Unit tests for the FastAPI layer (nyan/api/).

Uses FastAPI TestClient with mocked MongoDB collections — no real DB needed.
"""
import json
import os
import tempfile
from typing import Any, Dict, List
from unittest.mock import MagicMock, patch

import pytest
from fastapi.testclient import TestClient

from nyan.api.main import app
from nyan.api import deps
from nyan.api.routers import pipeline as pipeline_router


# ---------------------------------------------------------------------------
# Helpers / fixtures
# ---------------------------------------------------------------------------

def _make_doc(url: str = "https://t.me/test/1", issue: str = "main") -> Dict[str, Any]:
    return {
        "url": url,
        "channel_id": "test_channel",
        "post_id": 1,
        "views": 1000,
        "pub_time": 1700000000,
        "fetch_time": 1700000100,
        "images": [],
        "links": [],
        "videos": [],
        "reply_to": None,
        "forward_from": None,
        "channel_title": "Test Channel",
        "has_obscene": False,
        "patched_text": "Test news text",
        "groups": {},
        "issue": issue,
        "language": "ru",
        "category": "society",
        "category_scores": {"society": 0.9, "not_news": 0.1},
        "tokens": None,
        "embedding_key": "multilingual_e5_base",
        "version": 6,
        "pub_time_dt": None,
        "text": "Test news text full",
    }


def _make_cluster(clid: int = 1, issue: str = "main") -> Dict[str, Any]:
    doc = _make_doc(issue=issue)
    return {
        "clid": clid,
        "docs": [doc],
        "messages": [],
        "annotation_doc": doc,
        "first_doc": doc,
        "hash": "abc123",
        "diff": [],
        "is_important": False,
        "create_time": 1700000000,
    }


def _mock_col(records: List[Dict[str, Any]]) -> MagicMock:
    """Return a mock Collection that yields given records from .find()."""
    col = MagicMock()
    cursor = MagicMock()
    cursor.__iter__ = MagicMock(return_value=iter(records))
    col.find.return_value = cursor
    cursor.sort.return_value = cursor
    cursor.skip.return_value = cursor
    cursor.limit.return_value = cursor
    col.estimated_document_count.return_value = len(records)
    return col


@pytest.fixture
def client() -> TestClient:
    return TestClient(app)


@pytest.fixture(autouse=True)
def clear_overrides() -> Any:
    yield
    app.dependency_overrides.clear()


# ---------------------------------------------------------------------------
# GET /
# ---------------------------------------------------------------------------

def test_root(client: TestClient) -> None:
    resp = client.get("/")
    assert resp.status_code == 200
    assert resp.json()["status"] == "ok"


# ---------------------------------------------------------------------------
# GET /clusters
# ---------------------------------------------------------------------------

def test_list_clusters_empty(client: TestClient) -> None:
    col = _mock_col([])
    app.dependency_overrides[deps.get_clusters_col] = lambda: col
    resp = client.get("/clusters")
    assert resp.status_code == 200
    assert resp.json() == []


def test_list_clusters_returns_schema(client: TestClient) -> None:
    cluster_dict = _make_cluster(clid=42, issue="sports")
    col = _mock_col([cluster_dict])
    app.dependency_overrides[deps.get_clusters_col] = lambda: col
    resp = client.get("/clusters")
    assert resp.status_code == 200
    data = resp.json()
    assert len(data) == 1
    c = data[0]
    assert c["clid"] == 42
    assert "issues" in c
    assert "docs" in c
    assert len(c["docs"]) == 1
    assert c["docs"][0]["url"] == "https://t.me/test/1"
    assert c["docs"][0]["fetch_time"] == 1700000100
    assert c["docs"][0]["has_obscene"] is False


def test_list_clusters_issue_filter_pushes_to_query(client: TestClient) -> None:
    """When issue != 'main', the filter must be passed to .find() as docs.issue."""
    col = _mock_col([])
    app.dependency_overrides[deps.get_clusters_col] = lambda: col
    resp = client.get("/clusters?issue=sports")
    assert resp.status_code == 200
    call_args = col.find.call_args
    query = call_args[0][0]
    assert query.get("docs.issue") == "sports"


def test_list_clusters_issue_main_no_db_filter(client: TestClient) -> None:
    """issue=main should NOT add a docs.issue filter (all clusters have 'main')."""
    col = _mock_col([])
    app.dependency_overrides[deps.get_clusters_col] = lambda: col
    resp = client.get("/clusters?issue=main")
    assert resp.status_code == 200
    query = col.find.call_args[0][0]
    assert "docs.issue" not in query


def test_list_clusters_max_age_filter(client: TestClient) -> None:
    col = _mock_col([])
    app.dependency_overrides[deps.get_clusters_col] = lambda: col
    resp = client.get("/clusters?max_age_minutes=60")
    assert resp.status_code == 200
    query = col.find.call_args[0][0]
    assert "create_time" in query


def test_list_clusters_limit_upper_bound(client: TestClient) -> None:
    col = _mock_col([])
    app.dependency_overrides[deps.get_clusters_col] = lambda: col
    resp = client.get("/clusters?limit=99999")
    assert resp.status_code == 422


def test_list_clusters_max_age_upper_bound(client: TestClient) -> None:
    col = _mock_col([])
    app.dependency_overrides[deps.get_clusters_col] = lambda: col
    resp = client.get("/clusters?max_age_minutes=99999")
    assert resp.status_code == 422


# ---------------------------------------------------------------------------
# GET /clusters/{clid}
# ---------------------------------------------------------------------------

def test_get_cluster_found(client: TestClient) -> None:
    cluster_dict = _make_cluster(clid=7)
    col = MagicMock()
    col.find_one.return_value = cluster_dict
    app.dependency_overrides[deps.get_clusters_col] = lambda: col
    resp = client.get("/clusters/7")
    assert resp.status_code == 200
    assert resp.json()["clid"] == 7


def test_get_cluster_not_found(client: TestClient) -> None:
    col = MagicMock()
    col.find_one.return_value = None
    app.dependency_overrides[deps.get_clusters_col] = lambda: col
    resp = client.get("/clusters/9999")
    assert resp.status_code == 404


# ---------------------------------------------------------------------------
# GET /documents
# ---------------------------------------------------------------------------

def test_list_documents_empty(client: TestClient) -> None:
    col = _mock_col([])
    app.dependency_overrides[deps.get_documents_col] = lambda: col
    resp = client.get("/documents")
    assert resp.status_code == 200
    assert resp.json() == []


def test_list_documents_schema(client: TestClient) -> None:
    doc = _make_doc()
    col = _mock_col([doc])
    app.dependency_overrides[deps.get_documents_col] = lambda: col
    resp = client.get("/documents")
    assert resp.status_code == 200
    data = resp.json()
    assert len(data) == 1
    d = data[0]
    assert d["url"] == doc["url"]
    assert d["fetch_time"] == doc["fetch_time"]
    assert d["has_obscene"] is False
    assert "category_scores" in d


def test_list_documents_limit_upper_bound(client: TestClient) -> None:
    col = _mock_col([])
    app.dependency_overrides[deps.get_documents_col] = lambda: col
    resp = client.get("/documents?limit=99999")
    assert resp.status_code == 422


def test_list_documents_hours_upper_bound(client: TestClient) -> None:
    col = _mock_col([])
    app.dependency_overrides[deps.get_documents_col] = lambda: col
    resp = client.get("/documents?hours=99999")
    assert resp.status_code == 422


# ---------------------------------------------------------------------------
# GET /channels
# ---------------------------------------------------------------------------

# Minimal valid channels.json structure (Channels class requires emojis/colors/default_groups)
_CHANNELS_SKELETON = {
    "emojis": {"purple": "⚖️", "red": "🇷🇺", "other": "🌐"},
    "colors": {"purple": "#9b59b6", "red": "#e74c3c", "other": "#95a5a6"},
    "default_groups": {"main": "purple"},
}


def _write_channels_file(channels: list) -> str:
    data = {**_CHANNELS_SKELETON, "channels": channels}
    with tempfile.NamedTemporaryFile("w", suffix=".json", delete=False) as f:
        json.dump(data, f)
        return f.name


def test_list_channels(client: TestClient) -> None:
    tmp_path = _write_channels_file([
        {"name": "test_ch", "alias": "Test", "issue": "main", "disabled": False, "groups": {"main": "purple"}}
    ])
    try:
        app.dependency_overrides[deps.get_channels_path] = lambda: tmp_path
        resp = client.get("/channels")
        assert resp.status_code == 200
        data = resp.json()
        assert len(data) == 1
        assert data[0]["name"] == "test_ch"
        assert data[0]["disabled"] is False
    finally:
        os.unlink(tmp_path)


def test_disable_channel(client: TestClient) -> None:
    tmp_path = _write_channels_file([
        {"name": "my_ch", "alias": "My", "issue": "main", "disabled": False, "groups": {"main": "purple"}}
    ])
    try:
        app.dependency_overrides[deps.get_channels_path] = lambda: tmp_path
        resp = client.put("/channels/my_ch/disable")
        assert resp.status_code == 200
        assert resp.json()["disabled"] is True
        with open(tmp_path) as f:
            saved = json.load(f)
        assert saved["channels"][0]["disabled"] is True
    finally:
        os.unlink(tmp_path)


def test_enable_channel(client: TestClient) -> None:
    tmp_path = _write_channels_file([
        {"name": "my_ch", "alias": "My", "issue": "main", "disabled": True, "groups": {"main": "purple"}}
    ])
    try:
        app.dependency_overrides[deps.get_channels_path] = lambda: tmp_path
        resp = client.put("/channels/my_ch/enable")
        assert resp.status_code == 200
        assert resp.json()["disabled"] is False
    finally:
        os.unlink(tmp_path)


def test_disable_channel_not_found(client: TestClient) -> None:
    tmp_path = _write_channels_file([])
    try:
        app.dependency_overrides[deps.get_channels_path] = lambda: tmp_path
        resp = client.put("/channels/nonexistent/disable")
        assert resp.status_code == 404
    finally:
        os.unlink(tmp_path)


# ---------------------------------------------------------------------------
# GET /pipeline/status
# ---------------------------------------------------------------------------

def test_pipeline_status(client: TestClient) -> None:
    docs_col = MagicMock()
    docs_col.estimated_document_count.return_value = 100
    clusters_col = MagicMock()
    clusters_col.estimated_document_count.return_value = 10
    app.dependency_overrides[deps.get_documents_col] = lambda: docs_col
    app.dependency_overrides[deps.get_clusters_col] = lambda: clusters_col
    app.dependency_overrides[deps.get_mongo_config_path] = lambda: "configs/mongo_config.json"
    resp = client.get("/pipeline/status")
    assert resp.status_code == 200
    data = resp.json()
    assert data["documents_count"] == 100
    assert data["clusters_count"] == 10
    assert "daemon_running" in data
    assert "daemon_last_error" in data


# ---------------------------------------------------------------------------
# POST /pipeline/daemon
# ---------------------------------------------------------------------------

def test_run_daemon_conflict(client: TestClient) -> None:
    """Returns 409 when daemon iteration is already running."""
    app.dependency_overrides[deps.get_mongo_config_path] = lambda: "configs/mongo_config.json"
    pipeline_router._daemon_state["running"] = True
    try:
        resp = client.post("/pipeline/daemon")
        assert resp.status_code == 409
        assert "already running" in resp.json()["detail"]
    finally:
        pipeline_router._daemon_state["running"] = False


def test_run_daemon_starts(client: TestClient) -> None:
    """Returns 200 and schedules a background task when not already running."""
    app.dependency_overrides[deps.get_mongo_config_path] = lambda: "configs/mongo_config.json"
    pipeline_router._daemon_state["running"] = False

    with patch("nyan.api.routers.pipeline._run_daemon_iteration"):
        resp = client.post("/pipeline/daemon")
        assert resp.status_code == 200
        assert resp.json()["status"] == "started"

    # Reset state (background task won't actually run with TestClient in sync mode)
    pipeline_router._daemon_state["running"] = False


# ---------------------------------------------------------------------------
# POST /pipeline/crawl
# ---------------------------------------------------------------------------

def test_run_crawl_conflict(client: TestClient) -> None:
    """Returns 409 when crawl subprocess is already running."""
    mock_proc = MagicMock()
    mock_proc.poll.return_value = None  # process still running
    pipeline_router._crawl_proc = mock_proc
    try:
        resp = client.post("/pipeline/crawl")
        assert resp.status_code == 409
        assert "already running" in resp.json()["detail"]
    finally:
        pipeline_router._crawl_proc = None


def test_run_crawl_starts(client: TestClient) -> None:
    """Returns 200 with pid when crawl is not already running."""
    pipeline_router._crawl_proc = None
    mock_proc = MagicMock()
    mock_proc.pid = 12345
    with patch("subprocess.Popen", return_value=mock_proc):
        resp = client.post("/pipeline/crawl")
    assert resp.status_code == 200
    data = resp.json()
    assert data["status"] == "started"
    assert data["pid"] == "12345"
    pipeline_router._crawl_proc = None
