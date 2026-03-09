from typing import Any, Dict, List, Optional
from pydantic import BaseModel, Field


class DocumentSchema(BaseModel):
    url: str
    channel_id: str
    post_id: int
    views: int
    pub_time: int
    fetch_time: Optional[int] = None
    text: Optional[str] = None
    patched_text: Optional[str] = None
    channel_title: str = ""
    has_obscene: bool = False
    language: Optional[str] = None
    category: Optional[str] = None
    category_scores: Dict[str, float] = Field(default_factory=dict)
    issue: Optional[str] = None
    groups: Dict[str, str] = Field(default_factory=dict)
    images: List[str] = Field(default_factory=list)
    videos: List[str] = Field(default_factory=list)
    links: List[str] = Field(default_factory=list)
    forward_from: Optional[str] = None
    reply_to: Optional[str] = None

    model_config = {"from_attributes": True}


class MessageIdSchema(BaseModel):
    message_id: Optional[int] = None
    issue: str = ""


class ClusterSchema(BaseModel):
    clid: Optional[int]
    create_time: Optional[int]
    is_important: bool
    views: int
    pub_time: int
    issues: List[str]
    group: str
    channels: List[str]
    cropped_title: str
    images: List[str]
    videos: List[str]
    messages: List[MessageIdSchema]
    docs: List[DocumentSchema]
    diff: List[Dict[str, Any]] = Field(default_factory=list)


class ChannelSchema(BaseModel):
    name: str
    alias: str = ""
    issue: Optional[str] = None
    disabled: bool = False
    groups: Dict[str, str] = Field(default_factory=dict)
    master: Optional[str] = None


class PipelineStatusSchema(BaseModel):
    documents_count: int
    clusters_count: int
    mongo_config_path: str
    daemon_running: bool = False
    daemon_last_run: Optional[str] = None
    daemon_last_error: Optional[str] = None
