from __future__ import annotations

import html
import re
from pathlib import Path
from urllib.parse import parse_qs
from urllib.parse import urlparse
from urllib.parse import urlencode

import bleach
from bs4 import BeautifulSoup

from .kemono import parse_kemono_post_url

ALLOWED_TAGS = [
    "a",
    "abbr",
    "blockquote",
    "br",
    "code",
    "div",
    "em",
    "h1",
    "h2",
    "h3",
    "h4",
    "h5",
    "h6",
    "hr",
    "img",
    "li",
    "ol",
    "p",
    "pre",
    "span",
    "strong",
    "sub",
    "sup",
    "table",
    "tbody",
    "td",
    "th",
    "thead",
    "tr",
    "ul",
]
ALLOWED_ATTRIBUTES = {
    "*": ["class"],
    "a": ["href", "title", "target", "rel"],
    "img": ["src", "alt", "title"],
}
ALLOWED_PROTOCOLS = ["http", "https", "mailto"]


def render_post_content(
    raw_content: str | None,
    *,
    current_service: str,
    current_user_id: str,
    current_post_id: int,
    local_media_map: dict[str, str] | None = None,
    local_media_by_name: dict[str, str] | None = None,
) -> str:
    if not raw_content:
        return ""

    content = raw_content.strip()
    if not _looks_like_html(content):
        escaped = html.escape(content).replace("\r\n", "\n").replace("\n", "<br>\n")
        return _rewrite_kemono_links(
            escaped,
            current_service=current_service,
            current_user_id=current_user_id,
            current_post_id=current_post_id,
        )

    safe_html = bleach.clean(
        content,
        tags=ALLOWED_TAGS,
        attributes=ALLOWED_ATTRIBUTES,
        protocols=ALLOWED_PROTOCOLS,
        strip=True,
    )
    with_local_media = _rewrite_local_media_urls(
        safe_html,
        local_media_map=local_media_map or {},
        local_media_by_name=local_media_by_name or {},
    )
    with_inline_media = _expand_empty_image_links(with_local_media)
    return _rewrite_kemono_links(
        with_inline_media,
        current_service=current_service,
        current_user_id=current_user_id,
        current_post_id=current_post_id,
    )


def _rewrite_kemono_links(
    html_content: str,
    *,
    current_service: str,
    current_user_id: str,
    current_post_id: int,
) -> str:
    soup = BeautifulSoup(html_content, "html.parser")
    for link in soup.find_all("a"):
        href = link.get("href")
        if not href:
            continue
        try:
            ref = parse_kemono_post_url(href)
        except ValueError:
            continue

        query = {
            "service": ref.service,
            "post": ref.post_id,
            "from_post": str(current_post_id),
        }
        if ref.user_id:
            query["user"] = ref.user_id
        else:
            query["user"] = current_user_id
            query["assumed_from_context"] = "1"
        link["href"] = f"/links/resolve?{urlencode(query)}"
        link["title"] = "Open local copy or import this linked post"
    return str(soup)


def _expand_empty_image_links(html_content: str) -> str:
    soup = BeautifulSoup(html_content, "html.parser")
    for link in soup.find_all("a"):
        href = link.get("href")
        if not href:
            continue
        if link.find("img") is not None:
            continue
        if link.get_text(strip=True):
            continue
        if not _looks_like_image_url(href):
            continue

        filename = Path(urlparse(href).path).name
        alt_text = filename or "inline image"
        image = soup.new_tag("img", src=href, alt=alt_text, title=alt_text)
        link.clear()
        link.append(image)
        if not link.get("target"):
            link["target"] = "_blank"
        rel_values = set(link.get("rel", []))
        rel_values.update({"noopener", "noreferrer"})
        link["rel"] = sorted(rel_values)
    return str(soup)


def _looks_like_image_url(url: str) -> bool:
    parsed = urlparse(url)
    ext = Path(parsed.path.lower()).suffix
    if ext in {".jpg", ".jpeg", ".png", ".gif", ".webp", ".bmp", ".svg"}:
        return True
    if parsed.netloc.lower().endswith("fanbox.cc") and "/image/" in parsed.path.lower():
        return True
    return False


def _looks_like_html(content: str) -> bool:
    return bool(re.search(r"<[a-zA-Z][^>]*>", content))


def _rewrite_local_media_urls(
    html_content: str,
    *,
    local_media_map: dict[str, str],
    local_media_by_name: dict[str, str],
) -> str:
    if not local_media_map and not local_media_by_name:
        return html_content

    soup = BeautifulSoup(html_content, "html.parser")
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
            replacement = _find_local_media_replacement(
                raw_url.strip(),
                node=node,
                local_media_map=local_media_map,
                local_media_by_name=local_media_by_name,
            )
            if replacement:
                node[attr] = replacement
    return str(soup)


def _find_local_media_replacement(
    url: str,
    *,
    node: object | None,
    local_media_map: dict[str, str],
    local_media_by_name: dict[str, str],
) -> str | None:
    parsed = urlparse(url)
    filename = Path(parsed.path).name.lower()
    if filename:
        by_name = local_media_by_name.get(filename)
        if by_name:
            return by_name

    direct = local_media_map.get(url)
    if direct:
        return direct

    normalized_url = f"{parsed.scheme}://{parsed.netloc}{parsed.path}" if parsed.scheme and parsed.netloc else parsed.path
    direct_normalized = local_media_map.get(normalized_url)
    if direct_normalized:
        return direct_normalized

    alias_name = _anchor_alias_name(node, parsed.path)
    if alias_name:
        by_alias = local_media_by_name.get(alias_name)
        if by_alias:
            return by_alias

    # FANBOX image links sometimes put source URL in query parameters.
    query = parse_qs(parsed.query)
    for values in query.values():
        for candidate in values:
            nested = _find_local_media_replacement(
                candidate,
                node=node,
                local_media_map=local_media_map,
                local_media_by_name=local_media_by_name,
            )
            if nested:
                return nested
    return None


def _anchor_alias_name(node: object | None, path: str) -> str | None:
    if node is None or getattr(node, "name", None) != "a":
        return None
    text = getattr(node, "get_text", lambda *_args, **_kwargs: "")(" ", strip=True)
    if not isinstance(text, str) or not text.strip():
        return None

    suffix = Path(path).suffix.lower()
    label = text.strip().lower()
    label_suffix = Path(label).suffix.lower()
    if suffix and label_suffix != suffix and not _is_known_file_extension(label_suffix):
        return f"{label}{suffix}"
    return label


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
