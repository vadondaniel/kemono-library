from __future__ import annotations

import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

import requests

KEMONO_BASE = "https://kemono.cr"
KEMONO_HOSTS = {"kemono.cr", "kemono.su"}


@dataclass(frozen=True)
class KemonoPostRef:
    service: str
    post_id: str
    user_id: str | None = None

    @property
    def canonical_url(self) -> str:
        if self.user_id:
            return f"{KEMONO_BASE}/{self.service}/user/{self.user_id}/post/{self.post_id}"
        return f"{KEMONO_BASE}/{self.service}/post/{self.post_id}"

    def api_url(self, fallback_user_id: str | None = None) -> str:
        user_id = self.user_id or fallback_user_id
        if not user_id:
            raise ValueError("A user_id is required to build the Kemono API URL for this post.")
        return f"{KEMONO_BASE}/api/v1/{self.service}/user/{user_id}/post/{self.post_id}"


@dataclass(frozen=True)
class AttachmentCandidate:
    name: str
    remote_url: str
    kind: str


def parse_kemono_post_url(raw_url: str) -> KemonoPostRef:
    url = raw_url.strip()
    if not url:
        raise ValueError("Post URL is required.")
    if not re.match(r"^https?://", url, flags=re.IGNORECASE):
        url = f"https://{url}"

    parsed = urlparse(url)
    host = parsed.netloc.lower()
    if host not in KEMONO_HOSTS:
        raise ValueError("Only kemono.cr/kemono.su post links are supported.")

    parts = [p for p in parsed.path.split("/") if p]
    if len(parts) >= 5 and parts[1] == "user" and parts[3] == "post":
        return KemonoPostRef(service=parts[0], user_id=parts[2], post_id=parts[4])
    if len(parts) >= 3 and parts[1] == "post":
        return KemonoPostRef(service=parts[0], post_id=parts[2], user_id=None)
    raise ValueError("Unsupported Kemono URL shape. Use /{service}/user/{id}/post/{id} or /{service}/post/{id}.")


def fetch_post_json(post_ref: KemonoPostRef, fallback_user_id: str | None = None) -> dict[str, Any]:
    response = requests.get(post_ref.api_url(fallback_user_id=fallback_user_id), timeout=25)
    response.raise_for_status()
    payload = response.json()
    if not isinstance(payload, dict):
        raise ValueError("Unexpected API response. Expected JSON object.")
    return payload


def to_absolute_kemono_url(path_or_url: str) -> str:
    if path_or_url.startswith("http://") or path_or_url.startswith("https://"):
        return path_or_url
    if path_or_url.startswith("/"):
        return f"{KEMONO_BASE}{path_or_url}"
    return f"{KEMONO_BASE}/{path_or_url.lstrip('/')}"


def extract_attachments(post_payload: dict[str, Any]) -> list[AttachmentCandidate]:
    candidates: list[AttachmentCandidate] = []
    seen: set[str] = set()

    file_item = post_payload.get("file")
    if isinstance(file_item, dict):
        _append_attachment(candidates, seen, file_item, default_name="main-file", kind="file")

    attachments = post_payload.get("attachments")
    if isinstance(attachments, list):
        for item in attachments:
            if isinstance(item, dict):
                _append_attachment(
                    candidates,
                    seen,
                    item,
                    default_name="attachment",
                    kind="attachment",
                )
    return candidates


def sanitize_filename(filename: str) -> str:
    cleaned = re.sub(r"[^A-Za-z0-9._-]+", "_", filename).strip("._")
    return cleaned or "file"


def download_attachment(remote_url: str, destination: Path) -> None:
    destination.parent.mkdir(parents=True, exist_ok=True)
    with requests.get(remote_url, stream=True, timeout=60) as response:
        response.raise_for_status()
        with destination.open("wb") as handle:
            for chunk in response.iter_content(chunk_size=65536):
                if chunk:
                    handle.write(chunk)


def _append_attachment(
    out: list[AttachmentCandidate],
    seen: set[str],
    item: dict[str, Any],
    default_name: str,
    kind: str,
) -> None:
    raw_path = item.get("path") or item.get("url")
    if not isinstance(raw_path, str) or not raw_path.strip():
        return

    absolute_url = to_absolute_kemono_url(raw_path)
    if absolute_url in seen:
        return

    raw_name = item.get("name")
    if isinstance(raw_name, str) and raw_name.strip():
        name = raw_name.strip()
    else:
        name = Path(urlparse(absolute_url).path).name or default_name

    seen.add(absolute_url)
    out.append(AttachmentCandidate(name=name, remote_url=absolute_url, kind=kind))
