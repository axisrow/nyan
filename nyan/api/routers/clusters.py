from typing import List, Optional

from fastapi import APIRouter, Depends, HTTPException
from pymongo.collection import Collection

from nyan.api.deps import get_clusters_col
from nyan.api.schemas import ClusterSchema, DocumentSchema, MessageIdSchema
from nyan.clusters import Cluster
from nyan.util import get_current_ts

router = APIRouter(prefix="/clusters", tags=["clusters"])


def cluster_to_schema(cluster: Cluster) -> ClusterSchema:
    docs = [
        DocumentSchema(
            url=doc.url,
            channel_id=doc.channel_id,
            post_id=doc.post_id,
            views=doc.views,
            pub_time=doc.pub_time,
            text=doc.text,
            fetch_time=doc.fetch_time,
            patched_text=doc.patched_text,
            channel_title=doc.channel_title,
            has_obscene=doc.has_obscene,
            language=doc.language,
            category=doc.category,
            category_scores=doc.category_scores or {},
            issue=doc.issue,
            groups=doc.groups or {},
            images=list(doc.images),
            videos=list(doc.videos),
            links=list(doc.links),
            forward_from=doc.forward_from,
            reply_to=doc.reply_to,
        )
        for doc in cluster.docs
    ]
    messages = [
        MessageIdSchema(message_id=m.message_id, issue=m.issue)
        for m in cluster.messages
    ]
    return ClusterSchema(
        clid=cluster.clid,
        create_time=cluster.create_time,
        is_important=cluster.is_important,
        views=cluster.views,
        pub_time=cluster.pub_time,
        issues=cluster.issues,
        group=cluster.group,
        channels=cluster.channels,
        cropped_title=cluster.cropped_title,
        images=list(cluster.images),
        videos=list(cluster.videos),
        messages=messages,
        docs=docs,
        diff=cluster.saved_diff or [],
    )


@router.get("", response_model=List[ClusterSchema])
def list_clusters(
    issue: Optional[str] = None,
    limit: int = 20,
    offset: int = 0,
    max_age_minutes: Optional[int] = None,
    col: Collection = Depends(get_clusters_col),  # type: ignore[type-arg]
) -> List[ClusterSchema]:
    query = {}
    if max_age_minutes is not None:
        min_ts = get_current_ts() - max_age_minutes * 60
        query["create_time"] = {"$gte": min_ts}

    # "main" is added to every cluster's issues automatically — no DB filter needed
    if issue is not None and issue != "main":
        query["docs.issue"] = issue

    cursor = col.find(query).sort("create_time", -1).skip(offset).limit(limit)
    clusters = [cluster_to_schema(Cluster.fromdict(d)) for d in cursor]
    return clusters


@router.get("/{clid}", response_model=ClusterSchema)
def get_cluster(
    clid: int,
    col: Collection = Depends(get_clusters_col),  # type: ignore[type-arg]
) -> ClusterSchema:
    d = col.find_one({"clid": clid})
    if not d:
        raise HTTPException(status_code=404, detail="Cluster not found")
    cluster = Cluster.fromdict(d)
    return cluster_to_schema(cluster)
