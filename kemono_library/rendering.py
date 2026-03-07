from __future__ import annotations

import html
import re
from pathlib import Path
from urllib.parse import parse_qs
from urllib.parse import urlparse
from urllib.parse import urlencode

import bleach
from bs4 import BeautifulSoup
from bs4 import NavigableString
from bleach.linkifier import Linker
from bleach.linkifier import URL_RE

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
_CJK_FRIENDLY_URL_RE = re.compile(
    URL_RE.pattern.replace(r"\b(?<![@.])", r"(?:^|(?<=[^A-Za-z0-9_@.]))", 1),
    re.IGNORECASE | re.VERBOSE,
)
_URL_LINKER = Linker(url_re=_CJK_FRIENDLY_URL_RE)


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
        linkified = _linkify_urls(escaped)
        return _rewrite_kemono_links(
            linkified,
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
    linkified = _linkify_urls(with_local_media)
    with_inline_media = _expand_empty_image_links(linkified)
    with_grouped_promos = _group_promo_inserts(with_inline_media)
    return _rewrite_kemono_links(
        with_grouped_promos,
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
        ref = _parse_supported_post_link(href)
        if not ref:
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


def _parse_supported_post_link(href: str):
    try:
        return parse_kemono_post_url(href)
    except ValueError:
        pass

    parsed = urlparse(href)
    host = parsed.netloc.lower()
    if not host.endswith("fanbox.cc"):
        return None
    parts = [p for p in parsed.path.split("/") if p]
    if len(parts) >= 2 and parts[0] == "posts":
        post_id = parts[1]
        if post_id.isdigit():
            from .kemono import KemonoPostRef

            return KemonoPostRef(service="fanbox", post_id=post_id, user_id=None)
    if len(parts) >= 3 and parts[0].startswith("@") and parts[1] == "posts":
        post_id = parts[2]
        if post_id.isdigit():
            from .kemono import KemonoPostRef

            return KemonoPostRef(service="fanbox", post_id=post_id, user_id=None)
    return None


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


def _group_promo_inserts(html_content: str) -> str:
    soup = BeautifulSoup(html_content, "html.parser")
    headings = list(soup.find_all("h3"))
    for heading in headings:
        if not _is_promo_heading(heading):
            continue

        image_block = _next_nonempty_tag_sibling(heading)
        if image_block is None or image_block.name != "p" or not _is_image_only_paragraph(image_block):
            continue

        teaser_block = _next_nonempty_tag_sibling(image_block)
        if teaser_block is None or teaser_block.name != "p" or not teaser_block.get_text(" ", strip=True):
            continue

        container = soup.new_tag("section")
        container["class"] = ["post-promo-insert"]
        _append_class(image_block, "post-promo-image")
        _append_class(teaser_block, "post-promo-teaser")
        heading.insert_before(container)
        container.append(heading.extract())
        container.append(image_block.extract())
        container.append(teaser_block.extract())
    return str(soup)


def _is_promo_heading(node: object) -> bool:
    if getattr(node, "name", None) != "h3":
        return False
    link = node.find("a", href=True)
    if link is None:
        return False
    href = link.get("href")
    if not isinstance(href, str):
        return False
    return _parse_supported_post_link(href) is not None


def _next_nonempty_tag_sibling(node: object):
    sibling = getattr(node, "next_sibling", None)
    while sibling is not None:
        if isinstance(sibling, NavigableString):
            if sibling.strip():
                return None
            sibling = sibling.next_sibling
            continue
        if getattr(sibling, "name", None):
            return sibling
        sibling = getattr(sibling, "next_sibling", None)
    return None


def _is_image_only_paragraph(node: object) -> bool:
    if getattr(node, "name", None) != "p":
        return False
    for child in getattr(node, "children", []):
        if isinstance(child, NavigableString):
            if child.strip():
                return False
            continue
        child_name = getattr(child, "name", None)
        if child_name == "img":
            continue
        if child_name == "a":
            img = child.find("img")
            if img is None:
                return False
            if child.get_text(" ", strip=True):
                return False
            continue
        return False
    return node.find("img") is not None


def _append_class(node: object, class_name: str) -> None:
    if not hasattr(node, "attrs"):
        return
    classes = node.get("class")
    if isinstance(classes, list):
        if class_name not in classes:
            classes.append(class_name)
        node["class"] = classes
        return
    node["class"] = [class_name]


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


def _linkify_urls(html_content: str) -> str:
    return _URL_LINKER.linkify(html_content)


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
