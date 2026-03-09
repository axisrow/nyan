import json
import os
import shutil
import tempfile
from typing import List

from fastapi import APIRouter, Depends, HTTPException

from nyan.api.deps import get_channels_path
from nyan.api.schemas import ChannelSchema
from nyan.channels import Channels

router = APIRouter(prefix="/channels", tags=["channels"])


def _load_channels(path: str) -> Channels:
    try:
        return Channels(path)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to load channels: {e}")


@router.get("", response_model=List[ChannelSchema])
def list_channels(
    path: str = Depends(get_channels_path),
) -> List[ChannelSchema]:
    channels = _load_channels(path)
    return [
        ChannelSchema(
            name=ch.name,
            alias=ch.alias,
            issue=ch.issue,
            disabled=ch.disabled,
            groups=ch.groups,
            master=ch.master,
        )
        for _, ch in channels
    ]


def _set_disabled(name: str, disabled: bool, path: str) -> ChannelSchema:
    with open(path) as f:
        config = json.load(f)

    found = False
    for ch in config["channels"]:
        if ch["name"] == name:
            ch["disabled"] = disabled
            found = True
            break

    if not found:
        raise HTTPException(status_code=404, detail=f"Channel '{name}' not found")

    dir_name = os.path.dirname(os.path.abspath(path))
    with tempfile.NamedTemporaryFile("w", dir=dir_name, delete=False, suffix=".tmp") as tmp:
        json.dump(config, tmp, ensure_ascii=False, indent=2)
        tmp_path = tmp.name
    shutil.move(tmp_path, path)

    channels = _load_channels(path)
    ch = channels[name]
    return ChannelSchema(
        name=ch.name,
        alias=ch.alias,
        issue=ch.issue,
        disabled=ch.disabled,
        groups=ch.groups,
        master=ch.master,
    )


@router.put("/{name}/disable", response_model=ChannelSchema)
def disable_channel(
    name: str,
    path: str = Depends(get_channels_path),
) -> ChannelSchema:
    return _set_disabled(name, True, path)


@router.put("/{name}/enable", response_model=ChannelSchema)
def enable_channel(
    name: str,
    path: str = Depends(get_channels_path),
) -> ChannelSchema:
    return _set_disabled(name, False, path)
