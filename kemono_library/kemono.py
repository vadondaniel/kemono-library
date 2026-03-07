from __future__ import annotations

import json
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

import requests
from bs4 import BeautifulSoup

KEMONO_BASE = "https://kemono.cr"
KEMONO_HOSTS = {"kemono.cr"}
KEMONO_API_HEADERS = {
    # Kemono API quirk: some endpoints expect this Accept value.
    "Accept": "text/css",
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36"
    ),
    "Referer": f"{KEMONO_BASE}/",
    "Origin": KEMONO_BASE,
}


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
        raise ValueError("Only kemono.cr post links are supported.")

    parts = [p for p in parsed.path.split("/") if p]
    if len(parts) >= 5 and parts[1] == "user" and parts[3] == "post":
        return KemonoPostRef(service=parts[0], user_id=parts[2], post_id=parts[4])
    if len(parts) >= 3 and parts[1] == "post":
        return KemonoPostRef(service=parts[0], post_id=parts[2], user_id=None)
    raise ValueError("Unsupported Kemono URL shape. Use /{service}/user/{id}/post/{id} or /{service}/post/{id}.")


def fetch_post_json(post_ref: KemonoPostRef, fallback_user_id: str | None = None) -> dict[str, Any]:
    response = requests.get(
        post_ref.api_url(fallback_user_id=fallback_user_id),
        timeout=25,
        headers=KEMONO_API_HEADERS,
    )
    response.raise_for_status()
    try:
        payload = response.json()
    except ValueError:
        payload = json.loads(response.text)
    if not isinstance(payload, dict):
        raise ValueError("Unexpected API response. Expected JSON object.")
    return payload


def normalize_post_payload(payload: dict[str, Any]) -> dict[str, Any]:
    """Normalize Kemono API envelope payloads into a post-centric dict."""
    post = payload.get("post")
    if not isinstance(post, dict):
        return payload

    normalized = dict(post)
    merged_attachments: list[dict[str, Any]] = []
    _extend_unique_attachment_dicts(merged_attachments, post.get("attachments"))
    _extend_unique_attachment_dicts(merged_attachments, payload.get("attachments"))
    if merged_attachments:
        normalized["attachments"] = merged_attachments

    for key in ("previews", "videos", "props"):
        value = payload.get(key)
        if value is not None and key not in normalized:
            normalized[key] = value
    return normalized


def to_absolute_kemono_url(path_or_url: str) -> str:
    if path_or_url.startswith("http://") or path_or_url.startswith("https://"):
        return path_or_url
    if path_or_url.startswith("/"):
        return f"{KEMONO_BASE}{path_or_url}"
    return f"{KEMONO_BASE}/{path_or_url.lstrip('/')}"


def extract_attachments(post_payload: dict[str, Any]) -> list[AttachmentCandidate]:
    candidates: list[AttachmentCandidate] = []
    seen: set[str] = set()
    sources = [post_payload]
    nested_post = post_payload.get("post")
    if isinstance(nested_post, dict):
        sources.append(nested_post)
    declared_non_inline_names = _collect_declared_media_names(sources, post_payload)

    for source in sources:
        file_item = source.get("file")
        if isinstance(file_item, dict):
            _append_attachment(candidates, seen, file_item, default_name="main-file", kind="file")

        shared_file = source.get("shared_file")
        if isinstance(shared_file, dict):
            _append_attachment(
                candidates,
                seen,
                shared_file,
                default_name="shared-file",
                kind="shared_file",
            )

        attachments = source.get("attachments")
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

    videos = post_payload.get("videos")
    if isinstance(videos, list):
        for item in videos:
            if isinstance(item, dict):
                _append_attachment(
                    candidates,
                    seen,
                    item,
                    default_name="video",
                    kind="video",
                )
            elif isinstance(item, str) and item.strip():
                _append_url_attachment(
                    candidates,
                    seen,
                    to_absolute_kemono_url(item.strip()),
                    name=Path(urlparse(item.strip()).path).name or "video",
                    kind="video",
                )

    embed = nested_post.get("embed") if isinstance(nested_post, dict) else None
    if isinstance(embed, dict):
        for key in ("url", "src", "thumbnail", "thumbnail_url", "image"):
            value = embed.get(key)
            if isinstance(value, str) and value.strip():
                absolute = to_absolute_kemono_url(value.strip())
                if _looks_like_downloadable_url(absolute):
                    _append_url_attachment(
                        candidates,
                        seen,
                        absolute,
                        name=Path(urlparse(absolute).path).name or "embed-media",
                        kind="embed_media",
                    )

    content = _first_content(sources)
    if content:
        _append_inline_content_attachments(
            candidates,
            seen,
            content,
            reserved_names=declared_non_inline_names,
        )
    return _dedupe_non_inline_by_name(candidates)


def sanitize_filename(filename: str) -> str:
    cleaned = re.sub(r"[^A-Za-z0-9._-]+", "_", filename).strip("._")
    return cleaned or "file"


def download_attachment(remote_url: str, destination: Path) -> None:
    destination.parent.mkdir(parents=True, exist_ok=True)
    with requests.get(
        remote_url,
        stream=True,
        timeout=60,
        headers={
            "Accept": "*/*",
            "User-Agent": KEMONO_API_HEADERS["User-Agent"],
            "Referer": KEMONO_BASE + "/",
            "Origin": KEMONO_BASE,
        },
    ) as response:
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

    absolute_url = _resolve_attachment_url(item, raw_path)
    if absolute_url in seen:
        return

    raw_name = item.get("name")
    if isinstance(raw_name, str) and raw_name.strip():
        name = raw_name.strip()
    else:
        name = Path(urlparse(absolute_url).path).name or default_name

    seen.add(absolute_url)
    out.append(AttachmentCandidate(name=name, remote_url=absolute_url, kind=kind))


def _append_inline_content_attachments(
    out: list[AttachmentCandidate],
    seen: set[str],
    content: str,
    *,
    reserved_names: set[str] | None = None,
) -> None:
    soup = BeautifulSoup(content, "html.parser")
    existing_non_inline_names = {
        sanitize_filename(item.name).lower()
        for item in out
        if item.kind != "inline_media"
    }
    if reserved_names:
        existing_non_inline_names.update(reserved_names)
    url_attrs = (
        ("img", "src"),
        ("source", "src"),
        ("video", "src"),
        ("audio", "src"),
        ("a", "href"),
    )

    inline_counter = 1
    for tag_name, attr in url_attrs:
        for node in soup.find_all(tag_name):
            raw_url = node.get(attr)
            if not isinstance(raw_url, str) or not raw_url.strip():
                continue
            absolute_url = to_absolute_kemono_url(raw_url.strip())
            if tag_name == "a" and not _looks_like_downloadable_url(absolute_url):
                continue
            if tag_name != "a" and not _looks_like_media_url(absolute_url):
                continue
            filename = _infer_inline_name(node, absolute_url) or f"inline-{inline_counter}"
            normalized_name = sanitize_filename(filename).lower()
            if normalized_name in existing_non_inline_names:
                continue
            inline_counter += 1
            _append_url_attachment(
                out,
                seen,
                absolute_url,
                name=filename,
                kind="inline_media",
            )


def _append_url_attachment(
    out: list[AttachmentCandidate],
    seen: set[str],
    absolute_url: str,
    *,
    name: str,
    kind: str,
) -> None:
    if absolute_url in seen:
        return
    seen.add(absolute_url)
    out.append(AttachmentCandidate(name=name, remote_url=absolute_url, kind=kind))


def _first_content(sources: list[dict[str, Any]]) -> str | None:
    for source in sources:
        content = source.get("content")
        if isinstance(content, str) and content.strip():
            return content
    return None


def _looks_like_downloadable_url(url: str) -> bool:
    return _looks_like_media_url(url) or _looks_like_archive_url(url)


def _looks_like_media_url(url: str) -> bool:
    parsed = urlparse(url)
    path = parsed.path.lower()
    host = parsed.netloc.lower()
    ext = Path(path).suffix.lower()
    media_ext = {
        ".jpg",
        ".jpeg",
        ".png",
        ".gif",
        ".webp",
        ".bmp",
        ".svg",
        ".mp4",
        ".webm",
        ".m4v",
        ".mov",
        ".mp3",
        ".wav",
        ".ogg",
        ".flac",
    }
    if ext in media_ext:
        return True
    if path.startswith("/data/"):
        return True
    if host.endswith("fanbox.cc") and "/image/" in path:
        return True
    return False


def _looks_like_archive_url(url: str) -> bool:
    ext = Path(urlparse(url).path.lower()).suffix
    return ext in {".zip", ".rar", ".7z", ".tar", ".gz", ".pdf"}


def _extend_unique_attachment_dicts(target: list[dict[str, Any]], value: Any) -> None:
    if not isinstance(value, list):
        return
    seen_urls = {
        to_absolute_kemono_url(item.get("path") or item.get("url"))
        for item in target
        if isinstance(item, dict) and isinstance(item.get("path") or item.get("url"), str)
    }
    for item in value:
        if not isinstance(item, dict):
            continue
        raw_path = item.get("path") or item.get("url")
        if not isinstance(raw_path, str) or not raw_path.strip():
            continue
        absolute_url = to_absolute_kemono_url(raw_path)
        if absolute_url in seen_urls:
            continue
        target.append(item)
        seen_urls.add(absolute_url)


def _resolve_attachment_url(item: dict[str, Any], raw_path: str) -> str:
    if raw_path.startswith("http://") or raw_path.startswith("https://"):
        return raw_path

    server = item.get("server")
    if isinstance(server, str) and server.startswith(("http://", "https://")):
        if raw_path.startswith("/"):
            return f"{server.rstrip('/')}{raw_path}"
        return f"{server.rstrip('/')}/{raw_path.lstrip('/')}"
    return to_absolute_kemono_url(raw_path)


def _dedupe_non_inline_by_name(candidates: list[AttachmentCandidate]) -> list[AttachmentCandidate]:
    seen_urls: set[str] = set()
    ordered_keys: list[str] = []
    chosen_by_key: dict[str, AttachmentCandidate] = {}

    for candidate in candidates:
        if candidate.remote_url in seen_urls:
            continue
        seen_urls.add(candidate.remote_url)

        key = _candidate_name_key(candidate)
        existing = chosen_by_key.get(key)
        if existing is None:
            chosen_by_key[key] = candidate
            ordered_keys.append(key)
            continue

        if _candidate_priority(candidate) > _candidate_priority(existing):
            chosen_by_key[key] = candidate

    return [chosen_by_key[key] for key in ordered_keys]


def _infer_inline_name(node: Any, absolute_url: str) -> str:
    fallback = Path(urlparse(absolute_url).path).name
    if getattr(node, "name", None) != "a":
        return fallback

    text = node.get_text(" ", strip=True)
    if not text:
        return fallback

    # If anchor text is a label like "Break Room", treat it as filename stem.
    # Keep URL extension so it can dedupe with API attachment names.
    suffix = Path(urlparse(absolute_url).path).suffix
    if suffix and not Path(text).suffix:
        return f"{text}{suffix}"
    return text


def _candidate_name_key(candidate: AttachmentCandidate) -> str:
    normalized = sanitize_filename(candidate.name).lower()
    if normalized:
        return f"name:{normalized}"
    return f"url:{candidate.remote_url.lower()}"


def _candidate_priority(candidate: AttachmentCandidate) -> int:
    # Deduping preference by source reliability.
    # User-facing rule: API attachment/file sources must beat inline links.
    priorities = {
        "attachment": 60,
        "file": 55,
        "shared_file": 50,
        "video": 45,
        "embed_media": 40,
        "inline_media": 10,
    }
    return priorities.get(candidate.kind, 20)


def _collect_declared_media_names(
    sources: list[dict[str, Any]],
    root_payload: dict[str, Any],
) -> set[str]:
    names: set[str] = set()

    def add_name(value: Any) -> None:
        if isinstance(value, str) and value.strip():
            names.add(sanitize_filename(value).lower())

    for source in sources:
        file_item = source.get("file")
        if isinstance(file_item, dict):
            add_name(file_item.get("name"))

        shared_file = source.get("shared_file")
        if isinstance(shared_file, dict):
            add_name(shared_file.get("name"))

        attachments = source.get("attachments")
        if isinstance(attachments, list):
            for item in attachments:
                if isinstance(item, dict):
                    add_name(item.get("name"))

    videos = root_payload.get("videos")
    if isinstance(videos, list):
        for item in videos:
            if isinstance(item, dict):
                add_name(item.get("name"))

    nested_post = root_payload.get("post")
    if isinstance(nested_post, dict):
        embed = nested_post.get("embed")
        if isinstance(embed, dict):
            for key in ("name", "title"):
                add_name(embed.get(key))

    return names
