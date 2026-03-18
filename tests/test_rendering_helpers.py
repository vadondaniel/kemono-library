from bs4 import BeautifulSoup

from kemono_library.rendering import (
    _append_class,
    _expand_empty_image_links,
    _find_local_media_replacement,
    _group_promo_inserts,
    _is_image_only_paragraph,
    _is_promo_heading,
    _looks_like_image_url,
    _next_nonempty_tag_sibling,
    _normalize_fanbox_linkified_anchor,
    _normalize_frame_embed_anchor,
    _parse_supported_post_link,
    _rewrite_local_media_urls,
    _unique_kemono_by_extension,
)


class _DummyLink:
    def __init__(self, href, text):
        self._href = href
        self._text = text

    def get(self, key, default=None):
        if key == "href":
            return self._href
        return default

    def get_text(self, *_args, **_kwargs):
        return self._text


def test_normalize_fanbox_linkified_anchor_keeps_query_and_fragment():
    soup = BeautifulSoup(
        '<a href="https://www.fanbox.cc/posts/9187463?foo=1#sec">'
        "https://www.fanbox.cc/posts/9187463?foo=1#sec"
        "</a>",
        "html.parser",
    )
    link = soup.find("a")
    assert link is not None

    _normalize_fanbox_linkified_anchor(link)
    assert link["href"] == "https://www.fanbox.cc/posts/9187463?foo=1#sec"


def test_normalize_fanbox_linkified_anchor_guard_paths():
    _normalize_fanbox_linkified_anchor(_DummyLink(None, "x"))
    _normalize_fanbox_linkified_anchor(_DummyLink("https://www.fanbox.cc/posts/1", None))

    label_soup = BeautifulSoup(
        '<a href="https://www.fanbox.cc/posts/1">label-only</a>',
        "html.parser",
    )
    label_link = label_soup.find("a")
    assert label_link is not None
    _normalize_fanbox_linkified_anchor(label_link)
    assert label_link.get_text(strip=True) == "label-only"

    upper_scheme_soup = BeautifulSoup(
        '<a href="HTTPS://www.fanbox.cc/posts/1">HTTPS://www.fanbox.cc/posts/1</a>',
        "html.parser",
    )
    upper_scheme_link = upper_scheme_soup.find("a")
    assert upper_scheme_link is not None
    _normalize_fanbox_linkified_anchor(upper_scheme_link)
    assert upper_scheme_link["href"] == "HTTPS://www.fanbox.cc/posts/1"

    nested_content_soup = BeautifulSoup(
        (
            '<a href="https://www.fanbox.cc/posts/1suffix">'
            "<span>https://www.fanbox.cc/posts/1suffix</span>"
            "</a>"
        ),
        "html.parser",
    )
    nested_content_link = nested_content_soup.find("a")
    assert nested_content_link is not None
    _normalize_fanbox_linkified_anchor(nested_content_link)
    assert "suffix" in nested_content_link.get_text(" ", strip=True)


def test_normalize_frame_embed_anchor_guard_paths():
    _normalize_frame_embed_anchor(_DummyLink("", "frame embed"))
    _normalize_frame_embed_anchor(_DummyLink("https://example.com/x", None))


def test_parse_supported_post_link_handles_double_slash_fallback_routes():
    ref = _parse_supported_post_link("https://www.fanbox.cc/posts//123")
    assert ref is not None
    assert ref.service == "fanbox"
    assert ref.post_id == "123"
    assert ref.user_id is None

    creator_ref = _parse_supported_post_link("https://www.fanbox.cc/@creator/posts//456")
    assert creator_ref is not None
    assert creator_ref.service == "fanbox"
    assert creator_ref.post_id == "456"
    assert creator_ref.user_id is None


def test_expand_empty_image_links_and_group_promos_guard_paths():
    expanded = _expand_empty_image_links('<a></a><a href="https://example.com/post"></a>')
    assert "<img" not in expanded

    grouped = _group_promo_inserts(
        (
            '<h3><a href="https://www.fanbox.cc/posts/12">Promo</a></h3>'
            '<p><img src="https://cdn.example/cover.jpg"></p>'
            "<p>    </p>"
        )
    )
    assert "post-promo-insert" not in grouped


def test_helper_predicates_cover_negative_paths():
    soup = BeautifulSoup("<p>x</p><h3><a>missing</a></h3>", "html.parser")
    paragraph = soup.find("p")
    heading = soup.find("h3")
    assert paragraph is not None
    assert heading is not None
    assert not _is_promo_heading(paragraph)
    assert not _is_promo_heading(heading)

    assert not _is_image_only_paragraph(soup.new_tag("div"))
    assert not _is_image_only_paragraph(BeautifulSoup("<p>text<img src='x.jpg'></p>", "html.parser").p)
    assert not _is_image_only_paragraph(BeautifulSoup("<p><a href='x'></a></p>", "html.parser").p)
    assert not _is_image_only_paragraph(
        BeautifulSoup("<p><a href='x'><img src='x.jpg'>label</a></p>", "html.parser").p
    )
    assert not _is_image_only_paragraph(BeautifulSoup("<p><span>bad</span></p>", "html.parser").p)


def test_next_nonempty_tag_sibling_handles_text_and_unnamed_nodes():
    nonempty_soup = BeautifulSoup("<p>head</p>tail<span>next</span>", "html.parser")
    assert _next_nonempty_tag_sibling(nonempty_soup.p) is None

    whitespace_soup = BeautifulSoup("<p>head</p> \n<span>next</span>", "html.parser")
    next_tag = _next_nonempty_tag_sibling(whitespace_soup.p)
    assert getattr(next_tag, "name", None) == "span"

    class BareNode:
        def __init__(self, next_sibling=None):
            self.next_sibling = next_sibling

    class RootNode:
        def __init__(self, next_sibling=None):
            self.next_sibling = next_sibling

    assert _next_nonempty_tag_sibling(RootNode(BareNode(None))) is None


def test_append_class_and_image_url_helpers():
    _append_class(object(), "noop")

    soup = BeautifulSoup('<a class="existing">x</a>', "html.parser")
    link = soup.find("a")
    assert link is not None
    _append_class(link, "post-image-link")
    _append_class(link, "post-image-link")
    assert sorted(link.get("class", [])) == ["existing", "post-image-link"]

    assert _looks_like_image_url("https://assets.fanbox.cc/image/content-id")
    assert not _looks_like_image_url("https://example.com/plain")


def test_find_local_media_replacement_fallback_paths():
    rewritten = _rewrite_local_media_urls(
        "<img><a href=''></a>",
        local_media_map={"keep": "/keep"},
        local_media_by_name={},
        remote_media_by_name={},
    )
    assert "<img" in rewritten

    assert (
        _find_local_media_replacement(
            "https://cdn.example/files/a.jpg",
            node=None,
            local_media_map={"https://cdn.example/files/a.jpg": "/files/a.jpg"},
            local_media_by_name={},
            remote_media_by_name={},
        )
        == "/files/a.jpg"
    )

    assert (
        _find_local_media_replacement(
            "https://cdn.example/files/a.jpg?token=1",
            node=None,
            local_media_map={"https://cdn.example/files/a.jpg": "/files/a.jpg"},
            local_media_by_name={},
            remote_media_by_name={},
        )
        == "/files/a.jpg"
    )

    remote_by_name = {
        "vault.zip": "https://downloads.fanbox.cc/files/post/1/hash.zip",
        "__ext_unique__:.zip": "https://n2.kemono.cr/data/aa/bb/hash.zip?f=vault.zip",
    }
    assert (
        _find_local_media_replacement(
            "https://cdn.example/vault.zip",
            node=None,
            local_media_map={},
            local_media_by_name={},
            remote_media_by_name=remote_by_name,
        )
        == "https://n2.kemono.cr/data/aa/bb/hash.zip?f=vault.zip"
    )

    remote_by_sanitized = {
        "break_20room.zip": "https://downloads.fanbox.cc/files/post/1/hash.zip",
        "__ext_unique__:.zip": "https://n2.kemono.cr/data/aa/bb/hash.zip?f=Break%20Room.zip",
    }
    assert (
        _find_local_media_replacement(
            "https://cdn.example/Break%20Room.zip",
            node=None,
            local_media_map={},
            local_media_by_name={},
            remote_media_by_name=remote_by_sanitized,
        )
        == "https://n2.kemono.cr/data/aa/bb/hash.zip?f=Break%20Room.zip"
    )

    alias_soup = BeautifulSoup(
        (
            '<a href="https://downloads.fanbox.cc/files/post/10791194/H9U6jEFTAYx8c4nanzHWqQWv.zip">'
            "Break Room"
            "</a>"
        ),
        "html.parser",
    )
    alias_link = alias_soup.find("a")
    assert alias_link is not None
    remote_by_alias_sanitized = {
        "break_room.zip": "https://downloads.fanbox.cc/files/post/1/hash.zip",
        "__ext_unique__:.zip": "https://n2.kemono.cr/data/aa/bb/hash.zip?f=Break_Room.zip",
    }
    assert (
        _find_local_media_replacement(
            "https://downloads.fanbox.cc/files/post/10791194/H9U6jEFTAYx8c4nanzHWqQWv.zip",
            node=alias_link,
            local_media_map={},
            local_media_by_name={},
            remote_media_by_name=remote_by_alias_sanitized,
        )
        == "https://n2.kemono.cr/data/aa/bb/hash.zip?f=Break_Room.zip"
    )

    assert (
        _find_local_media_replacement(
            "https://downloads.fanbox.cc/redirect?file=https://cdn.example/path/pic.png",
            node=None,
            local_media_map={},
            local_media_by_name={"pic.png": "/files/pic.png"},
            remote_media_by_name={},
        )
        == "/files/pic.png"
    )

    assert _unique_kemono_by_extension({}, "") is None
