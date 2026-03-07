from __future__ import annotations

import html
import re
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
    return _rewrite_kemono_links(
        safe_html,
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


def _looks_like_html(content: str) -> bool:
    return bool(re.search(r"<[a-zA-Z][^>]*>", content))
