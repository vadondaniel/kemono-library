from __future__ import annotations

import hashlib
import json
import re
import shutil
import subprocess
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from urllib.parse import parse_qsl, urlparse

import requests
from bs4 import BeautifulSoup

KEMONO_BASE = "https://kemono.cr"
COOMER_BASE = "https://coomer.st"
SUPPORTED_ARCHIVE_HOSTS = {"kemono.cr", "coomer.st"}
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
COOMER_SERVICES = {
    "onlyfans",
    "fansly",
}


def _normalize_archive_host(raw_host: str | None) -> str | None:
    if not isinstance(raw_host, str):
        return None
    host = raw_host.strip().lower()
    if not host:
        return None
    if "@" in host:
        host = host.rsplit("@", 1)[-1]
    host = host.split(":", 1)[0]
    if host.startswith("www."):
        host = host[4:]
    return host or None


def _is_supported_archive_host(raw_host: str | None) -> bool:
    return _archive_root_host(raw_host) is not None


def _base_url_from_host(raw_host: str | None) -> str | None:
    host = _archive_root_host(raw_host)
    if not host:
        return None
    return f"https://{host}"


def _archive_root_host(raw_host: str | None) -> str | None:
    host = _normalize_archive_host(raw_host)
    if not host:
        return None
    for supported in SUPPORTED_ARCHIVE_HOSTS:
        if host == supported or host.endswith(f".{supported}"):
            return supported
    return None


def _normalize_archive_base(base_url: str | None) -> str | None:
    if not isinstance(base_url, str) or not base_url.strip():
        return None
    raw = base_url.strip()
    if not re.match(r"^https?://", raw, flags=re.IGNORECASE):
        raw = f"https://{raw.lstrip('/')}"
    parsed = urlparse(raw)
    return _base_url_from_host(parsed.netloc)


def _archive_base_for_service(service: str | None) -> str | None:
    if not isinstance(service, str) or not service.strip():
        return None
    if service.strip().lower() in COOMER_SERVICES:
        return COOMER_BASE
    return KEMONO_BASE


def _api_headers_for_base(base_url: str | None) -> dict[str, str]:
    resolved_base = _normalize_archive_base(base_url) or KEMONO_BASE
    return {
        **KEMONO_API_HEADERS,
        "Referer": f"{resolved_base}/",
        "Origin": resolved_base,
    }


def _payload_archive_base(payload: dict[str, Any]) -> str:
    if isinstance(payload, dict):
        direct = _normalize_archive_base(payload.get("__archive_base__"))  # type: ignore[arg-type]
        if direct:
            return direct
        service_guess = _archive_base_for_service(payload.get("service"))  # type: ignore[arg-type]
        if service_guess:
            return service_guess
        nested = payload.get("post")
        if isinstance(nested, dict):
            nested_base = _normalize_archive_base(nested.get("__archive_base__"))  # type: ignore[arg-type]
            if nested_base:
                return nested_base
            nested_service_guess = _archive_base_for_service(nested.get("service"))  # type: ignore[arg-type]
            if nested_service_guess:
                return nested_service_guess
    return KEMONO_BASE


def _creator_icon_candidates(service: str, user_id: str, *, preferred_base_url: str | None = None) -> list[str]:
    ordered: list[str] = []

    def add(base: str | None) -> None:
        normalized_base = _normalize_archive_base(base)
        if not normalized_base:
            return
        candidate = creator_icon_url(service, user_id, base_url=normalized_base)
        if candidate not in ordered:
            ordered.append(candidate)

    add(preferred_base_url)
    add(_archive_base_for_service(service))
    add(KEMONO_BASE)
    add(COOMER_BASE)
    if not ordered:
        ordered.append(creator_icon_url(service, user_id, base_url=KEMONO_BASE))
    return ordered


@dataclass(frozen=True)
class KemonoPostRef:
    service: str
    post_id: str
    user_id: str | None = None
    host: str = "kemono.cr"

    @property
    def base_url(self) -> str:
        normalized_host = _normalize_archive_host(self.host) or "kemono.cr"
        return f"https://{normalized_host}"

    @property
    def canonical_url(self) -> str:
        if self.user_id:
            return f"{self.base_url}/{self.service}/user/{self.user_id}/post/{self.post_id}"
        return f"{self.base_url}/{self.service}/post/{self.post_id}"

    def api_url(self, fallback_user_id: str | None = None) -> str:
        user_id = self.user_id or fallback_user_id
        if not user_id:
            raise ValueError("A user_id is required to build the Kemono API URL for this post.")
        return f"{self.base_url}/api/v1/{self.service}/user/{user_id}/post/{self.post_id}"


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
    host = _normalize_archive_host(parsed.netloc)
    if host not in SUPPORTED_ARCHIVE_HOSTS:
        raise ValueError("Only supported archive post links are supported.")

    parts = [p for p in parsed.path.split("/") if p]
    if len(parts) >= 5 and parts[1] == "user" and parts[3] == "post":
        return KemonoPostRef(service=parts[0], user_id=parts[2], post_id=parts[4], host=host)
    if len(parts) >= 3 and parts[1] == "post":
        return KemonoPostRef(service=parts[0], post_id=parts[2], user_id=None, host=host)
    raise ValueError("Unsupported archive URL shape. Use /{service}/user/{id}/post/{id} or /{service}/post/{id}.")


def fetch_post_json(post_ref: KemonoPostRef, fallback_user_id: str | None = None) -> dict[str, Any]:
    headers = _api_headers_for_base(post_ref.base_url)
    response = requests.get(
        post_ref.api_url(fallback_user_id=fallback_user_id),
        timeout=25,
        headers=headers,
    )
    response.raise_for_status()
    try:
        payload = response.json()
    except ValueError:
        payload = json.loads(response.text)
    if not isinstance(payload, dict):
        raise ValueError("Unexpected API response. Expected JSON object.")
    payload.setdefault("__archive_base__", post_ref.base_url)
    return payload


def normalize_post_payload(payload: dict[str, Any]) -> dict[str, Any]:
    """Normalize Kemono API envelope payloads into a post-centric dict."""
    post = payload.get("post")
    if not isinstance(post, dict):
        return payload

    normalized = dict(post)
    merged_attachments: list[dict[str, Any]] = []
    archive_base = _payload_archive_base(payload)
    _extend_unique_attachment_dicts(merged_attachments, post.get("attachments"), base_url=archive_base)
    _extend_unique_attachment_dicts(merged_attachments, payload.get("attachments"), base_url=archive_base)
    if merged_attachments:
        normalized["attachments"] = merged_attachments

    for key in ("previews", "videos", "props"):
        value = payload.get(key)
        if value is not None and key not in normalized:
            normalized[key] = value
    return normalized


def to_absolute_kemono_url(path_or_url: str, *, base_url: str | None = None) -> str:
    resolved_base = _normalize_archive_base(base_url) or KEMONO_BASE
    if path_or_url.startswith("http://") or path_or_url.startswith("https://"):
        return path_or_url
    if path_or_url.startswith("//"):
        return f"https:{path_or_url}"
    if path_or_url.startswith("/"):
        return f"{resolved_base}{path_or_url}"
    return f"{resolved_base}/{path_or_url.lstrip('/')}"


def extract_attachments(post_payload: dict[str, Any]) -> list[AttachmentCandidate]:
    candidates: list[AttachmentCandidate] = []
    seen: set[str] = set()
    archive_base = _payload_archive_base(post_payload)
    sources = [post_payload]
    nested_post = post_payload.get("post")
    if isinstance(nested_post, dict):
        sources.append(nested_post)
    declared_non_inline_names = _collect_declared_media_names(sources, post_payload)
    content = _first_content(sources)
    unnamed_attachment_aliases = _build_unnamed_attachment_aliases(
        sources,
        content,
        archive_base=archive_base,
    )

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
                archive_base=archive_base,
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
                archive_base=archive_base,
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
                        archive_base=archive_base,
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
                    archive_base=archive_base,
                )
            elif isinstance(item, str) and item.strip():
                _append_url_attachment(
                    candidates,
                    seen,
                    to_absolute_kemono_url(item.strip(), base_url=archive_base),
                    name=Path(urlparse(item.strip()).path).name or "video",
                    kind="video",
                )

    for source in sources:
        embed = source.get("embed")
        if isinstance(embed, dict):
            _append_embed_attachments(candidates, seen, embed, archive_base=archive_base)

    inline_name_keys: set[str] = set()
    if content:
        inline_name_keys = _collect_inline_name_keys(content, archive_base=archive_base)
        _append_inline_content_attachments(
            candidates,
            seen,
            content,
            reserved_names=declared_non_inline_names,
            archive_base=archive_base,
        )
    deduped = _dedupe_non_inline_by_name(candidates)
    return _relabel_inline_kinds(deduped, inline_name_keys)


def sanitize_filename(filename: str) -> str:
    cleaned = re.sub(r"[^A-Za-z0-9._-]+", "_", filename).strip("._")
    return cleaned or "file"


def _download_temp_path(destination: Path, *, marker: str) -> Path:
    # Keep temp filenames short and deterministic to avoid filesystem component limits.
    marker_clean = sanitize_filename(marker).lower() or "tmp"
    digest = hashlib.sha1(str(destination).encode("utf-8")).hexdigest()[:12]
    return destination.with_name(f".{marker_clean}_{digest}.part")


def download_attachment(remote_url: str, destination: Path) -> None:
    destination.parent.mkdir(parents=True, exist_ok=True)
    parsed = urlparse(remote_url)
    archive_base = _base_url_from_host(parsed.netloc) or KEMONO_BASE
    source_origin = f"{parsed.scheme}://{parsed.netloc}" if parsed.scheme and parsed.netloc else ""
    allow_insecure_retry = bool(parsed.netloc and not _is_supported_archive_host(parsed.netloc))
    base_headers = {
        "Accept": "*/*",
        "User-Agent": KEMONO_API_HEADERS["User-Agent"],
    }
    header_profiles = [
        {
            **base_headers,
            "Referer": archive_base + "/",
            "Origin": archive_base,
        },
        dict(base_headers),
    ]
    if source_origin:
        header_profiles.append(
            {
                **base_headers,
                "Referer": source_origin + "/",
                "Origin": source_origin,
            }
        )

    temp_destination = _download_temp_path(destination, marker="http")
    last_error: Exception | None = None

    for headers in header_profiles:
        try:
            with requests.get(remote_url, stream=True, timeout=60, headers=headers, verify=True) as response:
                response.raise_for_status()
                with temp_destination.open("wb") as handle:
                    for chunk in response.iter_content(chunk_size=65536):
                        if chunk:
                            handle.write(chunk)
            temp_destination.replace(destination)
            return
        except requests.exceptions.SSLError as ssl_exc:
            last_error = ssl_exc
            if not allow_insecure_retry:
                try:
                    if temp_destination.exists():
                        temp_destination.unlink()
                except OSError:
                    pass
                continue
            try:
                with requests.get(remote_url, stream=True, timeout=60, headers=headers, verify=False) as response:
                    response.raise_for_status()
                    with temp_destination.open("wb") as handle:
                        for chunk in response.iter_content(chunk_size=65536):
                            if chunk:
                                handle.write(chunk)
                temp_destination.replace(destination)
                return
            except Exception as insecure_exc:  # noqa: BLE001
                last_error = insecure_exc
                try:
                    if temp_destination.exists():
                        temp_destination.unlink()
                except OSError:
                    pass
        except Exception as exc:  # noqa: BLE001
            last_error = exc
            try:
                if temp_destination.exists():
                    temp_destination.unlink()
            except OSError:
                pass

    if _should_try_curl_fallback(remote_url, last_error):
        _download_attachment_with_curl(remote_url, destination)
        return

    if last_error is not None:
        raise last_error
    raise RuntimeError("Attachment download failed.")


def _should_try_curl_fallback(remote_url: str, error: Exception | None) -> bool:
    if error is None:
        return False
    parsed = urlparse(remote_url)
    host = parsed.netloc.lower().strip()
    if not host or _is_supported_archive_host(host):
        return False
    if not isinstance(error, requests.exceptions.HTTPError):
        return False
    response = getattr(error, "response", None)
    status = getattr(response, "status_code", None)
    return status in {403, 429}


def _download_attachment_with_curl(remote_url: str, destination: Path) -> None:
    curl_bin = shutil.which("curl") or shutil.which("curl.exe")
    if not curl_bin:
        raise RuntimeError("curl is not available for fallback download.")
    temp_destination = _download_temp_path(destination, marker="curl")
    command = [
        curl_bin,
        "--fail",
        "--location",
        "--silent",
        "--show-error",
        "--output",
        str(temp_destination),
        remote_url,
    ]
    try:
        subprocess.run(command, check=True, timeout=90)
        temp_destination.replace(destination)
    except Exception:  # noqa: BLE001
        try:
            if temp_destination.exists():
                temp_destination.unlink()
        except OSError:
            pass
        raise


def creator_icon_url(service: str, user_id: str, *, base_url: str | None = None) -> str:
    resolved_base = (
        _normalize_archive_base(base_url)
        or _archive_base_for_service(service)
        or KEMONO_BASE
    )
    host = urlparse(resolved_base).netloc
    return f"https://img.{host}/icons/{service}/{user_id}"


def download_creator_icon(
    service: str,
    user_id: str,
    icons_root: Path,
    *,
    base_url: str | None = None,
) -> tuple[str, Path | None]:
    candidates = _creator_icon_candidates(service, user_id, preferred_base_url=base_url)
    for remote_url in candidates:
        base_for_headers = _base_url_from_host(urlparse(remote_url).netloc) or KEMONO_BASE
        response = requests.get(
            remote_url,
            timeout=25,
            headers={
                "Accept": "image/*,*/*;q=0.8",
                "User-Agent": KEMONO_API_HEADERS["User-Agent"],
                "Referer": base_for_headers + "/",
                "Origin": base_for_headers,
            },
        )
        if response.status_code != 200 or not response.content:
            continue

        extension = _image_extension_from_content_type(response.headers.get("content-type"))
        destination = icons_root / f"{service}_{user_id}{extension}"
        destination.parent.mkdir(parents=True, exist_ok=True)
        destination.write_bytes(response.content)
        if destination.stat().st_size <= 0:
            continue
        return remote_url, destination
    return candidates[0], None


def _append_attachment(
    out: list[AttachmentCandidate],
    seen: set[str],
    item: dict[str, Any],
    default_name: str,
    kind: str,
    name_aliases: dict[str, str] | None = None,
    archive_base: str | None = None,
) -> None:
    raw_path = item.get("path") or item.get("url")
    if not isinstance(raw_path, str) or not raw_path.strip():
        return

    absolute_url = _resolve_attachment_url(item, raw_path, archive_base=archive_base)
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
    archive_base: str | None = None,
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
            absolute_url = to_absolute_kemono_url(raw_url.strip(), base_url=archive_base)
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


def _collect_inline_name_keys(content: str, *, archive_base: str | None = None) -> set[str]:
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
            absolute_url = to_absolute_kemono_url(raw_url.strip(), base_url=archive_base)
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


def _append_embed_attachments(
    out: list[AttachmentCandidate],
    seen: set[str],
    embed: dict[str, Any],
    *,
    archive_base: str | None = None,
) -> None:
    preferred_label = _first_embed_label(embed)
    for key in ("url", "src", "thumbnail", "thumbnail_url", "image"):
        value = embed.get(key)
        if isinstance(value, str) and value.strip():
            _append_embed_url_attachment(
                out,
                seen,
                value.strip(),
                preferred_label=preferred_label,
                archive_base=archive_base,
            )

    raw_html = embed.get("html")
    if isinstance(raw_html, str) and raw_html.strip():
        soup = BeautifulSoup(raw_html, "html.parser")
        for tag_name, attr in (
            ("iframe", "src"),
            ("img", "src"),
            ("source", "src"),
            ("video", "src"),
            ("audio", "src"),
            ("a", "href"),
        ):
            for node in soup.find_all(tag_name):
                raw_url = node.get(attr)
                if not isinstance(raw_url, str) or not raw_url.strip():
                    continue
                _append_embed_url_attachment(
                    out,
                    seen,
                    raw_url.strip(),
                    preferred_label=preferred_label,
                    archive_base=archive_base,
                )


def _append_embed_url_attachment(
    out: list[AttachmentCandidate],
    seen: set[str],
    raw_url: str,
    *,
    preferred_label: str | None,
    archive_base: str | None = None,
) -> None:
    absolute = to_absolute_kemono_url(raw_url, base_url=archive_base)
    parsed = urlparse(absolute)
    if parsed.scheme not in {"http", "https"} or not parsed.netloc:
        return

    if _looks_like_downloadable_url(absolute):
        _append_url_attachment(
            out,
            seen,
            absolute,
            name=Path(parsed.path).name or "embed-media",
            kind="embed_media",
        )
        return

    _append_url_attachment(
        out,
        seen,
        absolute,
        name=_embed_link_display_name(absolute, preferred_label=preferred_label),
        kind="embed_link",
    )


def _first_embed_label(embed: dict[str, Any]) -> str | None:
    for key in ("title", "subject", "name", "provider_name"):
        value = embed.get(key)
        if isinstance(value, str) and value.strip():
            return value.strip()
    return None


def _embed_link_display_name(url: str, *, preferred_label: str | None) -> str:
    if isinstance(preferred_label, str) and preferred_label.strip():
        return preferred_label.strip()
    parsed = urlparse(url)
    path_name = Path(parsed.path).name
    if path_name:
        return path_name
    if parsed.netloc:
        return parsed.netloc
    return "embed-link"


def _first_content(sources: list[dict[str, Any]]) -> str | None:
    for source in sources:
        content = source.get("content")
        if isinstance(content, str) and content.strip():
            return content
    return None


def _build_unnamed_attachment_aliases(
    sources: list[dict[str, Any]],
    content: str | None,
    *,
    archive_base: str | None = None,
) -> dict[str, str]:
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
            absolute_url = to_absolute_kemono_url(raw_url.strip(), base_url=archive_base)
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
            absolute_url = _resolve_attachment_url(item, raw_path, archive_base=archive_base)
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


def _extend_unique_attachment_dicts(
    target: list[dict[str, Any]],
    value: Any,
    *,
    base_url: str | None = None,
) -> None:
    if not isinstance(value, list):
        return
    seen_urls = {
        to_absolute_kemono_url(item.get("path") or item.get("url"), base_url=base_url)
        for item in target
        if isinstance(item, dict) and isinstance(item.get("path") or item.get("url"), str)
    }
    for item in value:
        if not isinstance(item, dict):
            continue
        raw_path = item.get("path") or item.get("url")
        if not isinstance(raw_path, str) or not raw_path.strip():
            continue
        absolute_url = to_absolute_kemono_url(raw_path, base_url=base_url)
        if absolute_url in seen_urls:
            continue
        target.append(item)
        seen_urls.add(absolute_url)


def _resolve_attachment_url(item: dict[str, Any], raw_path: str, *, archive_base: str | None = None) -> str:
    if raw_path.startswith("http://") or raw_path.startswith("https://"):
        return raw_path

    server = item.get("server")
    if isinstance(server, str) and server.startswith(("http://", "https://")):
        if raw_path.startswith("/"):
            return f"{server.rstrip('/')}{raw_path}"
        return f"{server.rstrip('/')}/{raw_path.lstrip('/')}"
    return to_absolute_kemono_url(raw_path, base_url=archive_base)


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
    if candidate.kind == "embed_link":
        return f"url:{candidate.remote_url.lower()}"
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
        "embed_link": 35,
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

    for source in sources:
        embed = source.get("embed")
        if isinstance(embed, dict):
            for key in ("name", "title", "subject"):
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
