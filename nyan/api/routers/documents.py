from typing import List, Optional

from fastapi import APIRouter, Depends
from pymongo.collection import Collection

from nyan.api.deps import get_documents_col
from nyan.api.schemas import DocumentSchema
from nyan.document import Document
from nyan.util import get_current_ts

router = APIRouter(prefix="/documents", tags=["documents"])


@router.get("", response_model=List[DocumentSchema])
def list_documents(
    channel_id: Optional[str] = None,
    limit: int = 20,
    offset: int = 0,
    hours: int = 24,
    col: Collection = Depends(get_documents_col),  # type: ignore[type-arg]
) -> List[DocumentSchema]:
    min_ts = get_current_ts() - hours * 3600
    query: dict = {"pub_time": {"$gte": min_ts}}
    if channel_id:
        query["channel_id"] = channel_id

    cursor = col.find(query).sort("pub_time", -1).skip(offset).limit(limit)
    result = []
    for d in cursor:
        doc = Document.fromdict(d)
        result.append(
            DocumentSchema(
                url=doc.url,
                channel_id=doc.channel_id,
                post_id=doc.post_id,
                views=doc.views,
                pub_time=doc.pub_time,
                fetch_time=doc.fetch_time,
                text=doc.text,
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
        )
    return result
