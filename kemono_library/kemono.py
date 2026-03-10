from __future__ import annotations

import json
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from urllib.parse import parse_qsl, urlparse

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
    content = _first_content(sources)
    unnamed_attachment_aliases = _build_unnamed_attachment_aliases(sources, content)

    for source in sources:
        file_item = source.get("file")
        if isinstance(file_item, dict):
            _append_attachment(
                candidates,
                seen,
                file_item,
                default_name="main-file",
                kind="thumbnail",
                name_aliases=unnamed_attachment_aliases,
            )

        shared_file = source.get("shared_file")
        if isinstance(shared_file, dict):
            _append_attachment(
                candidates,
                seen,
                shared_file,
                default_name="shared-file",
                kind="shared_file",
                name_aliases=unnamed_attachment_aliases,
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
                        name_aliases=unnamed_attachment_aliases,
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

    inline_name_keys: set[str] = set()
    if content:
        inline_name_keys = _collect_inline_name_keys(content)
        _append_inline_content_attachments(
            candidates,
            seen,
            content,
            reserved_names=declared_non_inline_names,
        )
    deduped = _dedupe_non_inline_by_name(candidates)
    return _relabel_inline_kinds(deduped, inline_name_keys)


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


def creator_icon_url(service: str, user_id: str) -> str:
    return f"https://img.kemono.cr/icons/{service}/{user_id}"


def download_creator_icon(service: str, user_id: str, icons_root: Path) -> tuple[str, Path | None]:
    remote_url = creator_icon_url(service, user_id)
    response = requests.get(
        remote_url,
        timeout=25,
        headers={
            "Accept": "image/*,*/*;q=0.8",
            "User-Agent": KEMONO_API_HEADERS["User-Agent"],
            "Referer": KEMONO_BASE + "/",
            "Origin": KEMONO_BASE,
        },
    )
    if response.status_code != 200 or not response.content:
        return remote_url, None

    extension = _image_extension_from_content_type(response.headers.get("content-type"))
    destination = icons_root / f"{service}_{user_id}{extension}"
    destination.parent.mkdir(parents=True, exist_ok=True)
    destination.write_bytes(response.content)
    if destination.stat().st_size <= 0:
        return remote_url, None
    return remote_url, destination


def _append_attachment(
    out: list[AttachmentCandidate],
    seen: set[str],
    item: dict[str, Any],
    default_name: str,
    kind: str,
    name_aliases: dict[str, str] | None = None,
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
    elif name_aliases and absolute_url in name_aliases:
        name = name_aliases[absolute_url]
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
            if not _should_keep_inline_url(tag_name, absolute_url):
                continue
            inferred_name = _infer_inline_name(node, absolute_url) or f"inline-{inline_counter}"
            filename = _with_default_inline_extension(tag_name, absolute_url, inferred_name)
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


def _collect_inline_name_keys(content: str) -> set[str]:
    soup = BeautifulSoup(content, "html.parser")
    name_keys: set[str] = set()
    url_attrs = (
        ("img", "src"),
        ("source", "src"),
        ("video", "src"),
        ("audio", "src"),
        ("a", "href"),
    )

    for tag_name, attr in url_attrs:
        for node in soup.find_all(tag_name):
            raw_url = node.get(attr)
            if not isinstance(raw_url, str) or not raw_url.strip():
                continue
            absolute_url = to_absolute_kemono_url(raw_url.strip())
            if not _should_keep_inline_url(tag_name, absolute_url):
                continue

            url_name = Path(urlparse(absolute_url).path).name
            if url_name:
                normalized_url_name = _with_default_inline_extension(tag_name, absolute_url, url_name)
                name_keys.add(sanitize_filename(normalized_url_name).lower())

            inline_name = _infer_inline_name(node, absolute_url)
            if inline_name:
                normalized_inline_name = _with_default_inline_extension(tag_name, absolute_url, inline_name)
                name_keys.add(sanitize_filename(normalized_inline_name).lower())
    return name_keys


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


def _build_unnamed_attachment_aliases(sources: list[dict[str, Any]], content: str | None) -> dict[str, str]:
    if not content:
        return {}

    soup = BeautifulSoup(content, "html.parser")
    inline_filenames: list[str] = []
    seen_inline_names: set[str] = set()
    url_attrs = (
        ("img", "src"),
        ("source", "src"),
        ("video", "src"),
        ("audio", "src"),
        ("a", "href"),
    )
    for tag_name, attr in url_attrs:
        for node in soup.find_all(tag_name):
            raw_url = node.get(attr)
            if not isinstance(raw_url, str) or not raw_url.strip():
                continue
            absolute_url = to_absolute_kemono_url(raw_url.strip())
            if not _should_keep_inline_url(tag_name, absolute_url):
                continue
            filename = Path(urlparse(absolute_url).path).name
            filename = _with_default_inline_extension(tag_name, absolute_url, filename)
            normalized = filename.lower()
            if not filename or normalized in seen_inline_names:
                continue
            seen_inline_names.add(normalized)
            inline_filenames.append(filename)

    if len(inline_filenames) != 1:
        return {}

    unnamed_attachment_urls: list[str] = []
    seen_attachment_urls: set[str] = set()
    for source in sources:
        attachments = source.get("attachments")
        if not isinstance(attachments, list):
            continue
        for item in attachments:
            if not isinstance(item, dict):
                continue
            raw_name = item.get("name")
            if isinstance(raw_name, str) and raw_name.strip():
                continue
            raw_path = item.get("path") or item.get("url")
            if not isinstance(raw_path, str) or not raw_path.strip():
                continue
            absolute_url = _resolve_attachment_url(item, raw_path)
            if absolute_url in seen_attachment_urls:
                continue
            seen_attachment_urls.add(absolute_url)
            unnamed_attachment_urls.append(absolute_url)

    if len(unnamed_attachment_urls) != 1:
        return {}

    return {unnamed_attachment_urls[0]: inline_filenames[0]}


def _looks_like_downloadable_url(url: str) -> bool:
    return _looks_like_media_url(url) or _looks_like_archive_url(url)


def _should_keep_inline_url(tag_name: str, absolute_url: str) -> bool:
    if tag_name == "a":
        return _looks_like_downloadable_url(absolute_url)
    if tag_name == "img":
        parsed = urlparse(absolute_url)
        return parsed.scheme in {"http", "https"} and bool(parsed.netloc)
    return _looks_like_media_url(absolute_url)


def _with_default_inline_extension(tag_name: str, absolute_url: str, name: str) -> str:
    if not isinstance(name, str):
        return name
    cleaned = name.strip()
    if not cleaned or Path(cleaned).suffix:
        return cleaned
    suffix = _default_inline_extension(tag_name, absolute_url)
    if not suffix:
        return cleaned
    return f"{cleaned}{suffix}"


def _default_inline_extension(tag_name: str, absolute_url: str) -> str:
    inferred = _infer_extension_hint_from_url(absolute_url)
    if inferred:
        return inferred
    if tag_name == "img":
        return ".jpg"
    if tag_name in {"video", "source"}:
        return ".mp4"
    if tag_name == "audio":
        return ".mp3"
    return ""


def _infer_extension_hint_from_url(absolute_url: str) -> str:
    parsed = urlparse(absolute_url)
    for key, value in parse_qsl(parsed.query, keep_blank_values=False):
        key_norm = key.strip().lower()
        value_norm = value.strip().lower()
        if not key_norm or not value_norm:
            continue

        if key_norm in {"f", "file", "filename", "name", "download", "fn"}:
            ext = Path(value_norm).suffix.lower()
            if _is_known_file_extension(ext):
                return ext
            continue

        if key_norm in {"format", "fm", "ext", "extension", "type", "mime", "content_type"}:
            ext = _extension_from_mime_or_format(value_norm)
            if ext:
                return ext
    return ""


def _extension_from_mime_or_format(value: str) -> str:
    if not isinstance(value, str):
        return ""
    normalized = value.strip().lower()
    if not normalized:
        return ""
    if normalized.startswith(".") and _is_known_file_extension(normalized):
        return normalized

    mime_mapping = {
        "image/jpeg": ".jpg",
        "image/jpg": ".jpg",
        "image/png": ".png",
        "image/gif": ".gif",
        "image/webp": ".webp",
        "image/bmp": ".bmp",
        "image/svg+xml": ".svg",
        "image/avif": ".avif",
        "video/mp4": ".mp4",
        "video/webm": ".webm",
        "audio/mpeg": ".mp3",
        "audio/mp3": ".mp3",
        "audio/ogg": ".ogg",
        "audio/wav": ".wav",
    }
    if normalized in mime_mapping:
        return mime_mapping[normalized]

    format_mapping = {
        "jpeg": ".jpg",
        "jpg": ".jpg",
        "png": ".png",
        "gif": ".gif",
        "webp": ".webp",
        "bmp": ".bmp",
        "svg": ".svg",
        "avif": ".avif",
        "mp4": ".mp4",
        "webm": ".webm",
        "mp3": ".mp3",
        "ogg": ".ogg",
        "wav": ".wav",
    }
    return format_mapping.get(normalized, "")


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
        ".avif",
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


def _relabel_inline_kinds(
    candidates: list[AttachmentCandidate],
    inline_name_keys: set[str],
) -> list[AttachmentCandidate]:
    if not inline_name_keys:
        return [
            AttachmentCandidate(
                name=item.name,
                remote_url=item.remote_url,
                kind="inline_only" if item.kind == "inline_media" else item.kind,
            )
            for item in candidates
        ]

    relabeled: list[AttachmentCandidate] = []
    for item in candidates:
        name_key = sanitize_filename(item.name).lower()
        if item.kind == "inline_media":
            relabeled.append(
                AttachmentCandidate(name=item.name, remote_url=item.remote_url, kind="inline_only")
            )
            continue
        if name_key and name_key in inline_name_keys:
            relabeled.append(
                AttachmentCandidate(name=item.name, remote_url=item.remote_url, kind="inline_media")
            )
            continue
        relabeled.append(item)
    return relabeled


def _infer_inline_name(node: Any, absolute_url: str) -> str:
    fallback = Path(urlparse(absolute_url).path).name
    if getattr(node, "name", None) != "a":
        return fallback

    text = node.get_text(" ", strip=True)
    if not text:
        return fallback

    # If anchor text is a label like "Break Room" or "Artwork No.34",
    # treat it as filename stem and keep URL extension for dedupe.
    url_suffix = Path(urlparse(absolute_url).path).suffix.lower()
    text_suffix = Path(text).suffix.lower()
    if url_suffix and text_suffix != url_suffix and not _is_known_file_extension(text_suffix):
        if _should_preserve_anchor_suffix(url_suffix):
            return f"{text}{url_suffix}"
        return text
    return text


def _should_preserve_anchor_suffix(url_suffix: str) -> bool:
    return url_suffix in {
        ".zip",
        ".rar",
        ".7z",
        ".tar",
        ".gz",
        ".pdf",
        ".txt",
        ".json",
        ".csv",
    }


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
        "thumbnail": 55,
        "file": 55,
        "shared_file": 50,
        "video": 45,
        "embed_media": 40,
        "inline_media": 10,
        "inline_only": 10,
    }
    return priorities.get(candidate.kind, 20)


def _is_known_file_extension(suffix: str) -> bool:
    if not suffix:
        return False
    known = {
        ".jpg",
        ".jpeg",
        ".png",
        ".gif",
        ".webp",
        ".bmp",
        ".svg",
        ".avif",
        ".mp4",
        ".webm",
        ".m4v",
        ".mov",
        ".mp3",
        ".wav",
        ".ogg",
        ".flac",
        ".zip",
        ".rar",
        ".7z",
        ".tar",
        ".gz",
        ".pdf",
        ".txt",
        ".json",
        ".csv",
    }
    return suffix in known


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


def _image_extension_from_content_type(content_type: str | None) -> str:
    if not isinstance(content_type, str):
        return ".img"
    normalized = content_type.split(";")[0].strip().lower()
    mapping = {
        "image/jpeg": ".jpg",
        "image/jpg": ".jpg",
        "image/png": ".png",
        "image/webp": ".webp",
        "image/gif": ".gif",
        "image/svg+xml": ".svg",
        "image/avif": ".avif",
    }
    return mapping.get(normalized, ".img")
