from kemono_library.kemono import (
    KemonoPostRef,
    extract_attachments,
    fetch_post_json,
    normalize_post_payload,
    parse_kemono_post_url,
)


def test_parse_full_kemono_url():
    ref = parse_kemono_post_url("https://kemono.cr/fanbox/user/70479526/post/10791194")
    assert ref.service == "fanbox"
    assert ref.user_id == "70479526"
    assert ref.post_id == "10791194"
    assert ref.api_url() == "https://kemono.cr/api/v1/fanbox/user/70479526/post/10791194"


def test_parse_short_kemono_url():
    ref = parse_kemono_post_url("https://kemono.cr/fanbox/post/9581944")
    assert ref.service == "fanbox"
    assert ref.user_id is None
    assert ref.post_id == "9581944"


def test_extract_attachments_collects_main_and_list():
    payload = {
        "file": {"name": "cover.png", "path": "/data/abc/cover.png"},
        "attachments": [
            {"name": "page-1.jpg", "path": "/data/abc/page-1.jpg"},
            {"name": "doc.zip", "url": "https://files.example/doc.zip"},
        ],
    }
    items = extract_attachments(payload)
    assert len(items) == 3
    assert items[0].name == "cover.png"
    assert items[0].remote_url == "https://kemono.cr/data/abc/cover.png"


def test_extract_attachments_ignores_previews():
    payload = {
        "previews": [
            {
                "type": "thumbnail",
                "server": "https://n1.kemono.cr",
                "name": "thumb.jpeg",
                "path": "/aa/bb/thumb.jpg",
            }
        ]
    }
    items = extract_attachments(payload)
    assert items == []


def test_extract_attachments_ignores_multiple_previews():
    payload = {
        "previews": [
            {"type": "thumbnail", "name": "thumb-1.jpg", "path": "/p/a.jpg"},
            {"type": "thumbnail", "name": "thumb-2.jpg", "path": "/p/b.jpg"},
            {"type": "sample", "name": "sample.jpg", "path": "/p/c.jpg"},
        ]
    }
    items = extract_attachments(payload)
    assert items == []


def test_extract_attachments_collects_from_envelope_shape():
    payload = {
        "post": {
            "file": {"name": "main.jpg", "path": "/data/x/main.jpg"},
            "attachments": [{"name": "inside.txt", "path": "/data/x/inside.txt"}],
        },
        "attachments": [{"name": "top.zip", "path": "/data/x/top.zip"}],
    }
    items = extract_attachments(payload)
    names = [item.name for item in items]
    assert names == ["top.zip", "main.jpg", "inside.txt"]


def test_extract_attachments_dedupes_non_inline_same_filename():
    payload = {
        "post": {"attachments": [{"name": "dup.zip", "path": "/a/first.zip"}]},
        "attachments": [{"name": "dup.zip", "path": "/b/second.zip"}],
    }
    items = extract_attachments(payload)
    assert len(items) == 1
    assert items[0].name == "dup.zip"


def test_extract_attachments_file_used_for_cover_without_previews():
    payload = {
        "post": {"file": {"name": "same.jpg", "path": "/data/full/same.jpg"}},
        "previews": [{"type": "thumbnail", "name": "same.jpg", "path": "/data/thumb/same.jpg"}],
    }
    items = extract_attachments(payload)
    assert len(items) == 1
    assert items[0].kind == "thumbnail"


def test_extract_attachments_prefers_attachment_over_file_on_name_collision():
    payload = {
        "post": {
            "file": {"name": "same.jpg", "path": "/data/full/same.jpg"},
            "attachments": [{"name": "same.jpg", "path": "/data/att/same.jpg"}],
        }
    }
    items = extract_attachments(payload)
    assert len(items) == 1
    assert items[0].kind == "attachment"


def test_normalize_post_payload_unwraps_envelope():
    payload = {
        "post": {
            "title": "T",
            "content": "C",
            "user": "U",
            "attachments": [{"name": "inner", "path": "/data/inner.jpg"}],
        },
        "attachments": [{"name": "a", "path": "/data/a"}],
        "previews": [{"id": 1}],
    }
    normalized = normalize_post_payload(payload)
    assert normalized["title"] == "T"
    assert normalized["content"] == "C"
    assert normalized["user"] == "U"
    assert isinstance(normalized["attachments"], list)
    assert len(normalized["attachments"]) == 2
    assert isinstance(normalized["previews"], list)


def test_fetch_post_json_uses_css_accept(monkeypatch):
    captured_headers = {}

    class FakeResponse:
        text = '{"title":"ok"}'

        @staticmethod
        def raise_for_status():
            return None

        @staticmethod
        def json():
            raise ValueError("not declared as json content-type")

    def fake_get(url, timeout, headers):  # noqa: ARG001
        captured_headers.update(headers)
        return FakeResponse()

    monkeypatch.setattr("kemono_library.kemono.requests.get", fake_get)
    payload = fetch_post_json(KemonoPostRef(service="fanbox", user_id="1", post_id="2"))

    assert payload["title"] == "ok"
    assert captured_headers["Accept"] == "text/css"


def test_extract_attachments_includes_inline_media():
    payload = {
        "post": {
            "content": (
                '<p><img src="/data/local/image-a.jpg"></p>'
                '<a href="https://downloads.fanbox.cc/image/file-b.png">b</a>'
                '<a href="https://kemono.cr/fanbox/post/111">not-media</a>'
            )
        }
    }
    items = extract_attachments(payload)
    urls = [item.remote_url for item in items]
    assert "https://kemono.cr/data/local/image-a.jpg" in urls
    assert "https://downloads.fanbox.cc/image/file-b.png" in urls
    assert "https://kemono.cr/fanbox/post/111" not in urls
    assert all(item.kind == "inline_only" for item in items)


def test_extract_attachments_dedupes_same_filename_between_sources():
    payload = {
        "post": {
            "attachments": [
                {"name": "same-name.jpeg", "path": "/aa/bb/hash-1.jpg"},
            ],
            "content": '<a href="https://downloads.fanbox.cc/images/post/1/same-name.jpeg"></a>',
        }
    }
    items = extract_attachments(payload)
    matching = [item for item in items if item.name == "same-name.jpeg"]
    assert len(matching) == 1
    assert matching[0].kind == "inline_media"


def test_extract_attachments_dedupes_inline_anchor_text_file_name():
    payload = {
        "post": {
            "attachments": [{"name": "Break Room.zip", "path": "/be/0d/hash.zip"}],
            "content": (
                '<p><a href="https://downloads.fanbox.cc/files/post/10791194/'
                'H9U6jEFTAYx8c4nanzHWqQWv.zip" rel="noopener noreferrer">Break Room</a></p>'
            ),
        }
    }
    items = extract_attachments(payload)
    names = [item.name for item in items]
    assert names.count("Break Room.zip") == 1
    assert any(item.kind == "inline_media" for item in items)


def test_extract_attachments_dedupes_inline_anchor_with_numeric_suffix_label():
    payload = {
        "post": {
            "attachments": [{"name": "Artwork No.34.zip", "path": "/aa/bb/art.zip"}],
            "content": (
                '<a href="https://downloads.fanbox.cc/files/post/11441751/'
                'EZNQnKOMdH7j94svQWqmIaPo.zip" rel="noopener noreferrer">Artwork No.34</a>'
            ),
        }
    }
    items = extract_attachments(payload)
    names = [item.name for item in items]
    assert names.count("Artwork No.34.zip") == 1
    assert any(item.kind == "inline_media" for item in items)


def test_extract_attachments_skips_inline_when_name_declared_in_api_attachments():
    payload = {
        "post": {
            "attachments": [
                {"name": "eO8wzPLjankw59mg6YeTMzxN.jpeg", "path": "/c2/4f/shared.jpg"},
                {"name": "vh5E4UKF5F5EUzIIXGjVkRkc.jpeg", "path": "/c2/4f/shared.jpg"},
            ],
            "content": (
                '<a href="https://downloads.fanbox.cc/images/post/10791194/'
                'vh5E4UKF5F5EUzIIXGjVkRkc.jpeg" rel="noopener noreferrer"></a>'
            ),
        }
    }
    items = extract_attachments(payload)
    assert all(item.kind != "inline_media" for item in items)
    assert any(item.kind == "attachment" for item in items)


def test_extract_attachments_aliases_single_unnamed_attachment_to_single_inline_filename():
    payload = {
        "post": {
            "file": {
                "name": "e0ba6fda-8a0b-403c-88e5-9ceb9998fb8e.jpg",
                "path": "/34/1c/341cd3e2247f877f64c0a513fee2937ff51991ac0c345657c7228a90ec752045.jpg",
            },
            "attachments": [
                {"path": "/de/29/de29e172fef8fde94aeac3170bb40855d3d8b153e09377eb26cf02910ac4db2e.jpg"}
            ],
            "content": (
                '<a href="https://downloads.fanbox.cc/images/post/1566363/'
                'n5tyt6OGwuxG51nInjj0kMcW.jpeg" rel="noopener noreferrer"></a>'
            ),
        }
    }
    items = extract_attachments(payload)
    matching = [item for item in items if item.name == "n5tyt6OGwuxG51nInjj0kMcW.jpeg"]
    assert len(matching) == 1
    assert matching[0].remote_url == "https://kemono.cr/de/29/de29e172fef8fde94aeac3170bb40855d3d8b153e09377eb26cf02910ac4db2e.jpg"
    assert matching[0].kind == "inline_media"


def test_extract_attachments_collects_videos_and_embed_media():
    payload = {
        "videos": [
            {"name": "clip.mp4", "path": "/data/v/clip.mp4"},
            "https://cdn.example/video2.webm",
        ],
        "post": {"embed": {"thumbnail_url": "https://cdn.example/thumb.jpg"}},
    }
    items = extract_attachments(payload)
    urls = {item.remote_url: item.kind for item in items}
    assert urls["https://kemono.cr/data/v/clip.mp4"] == "video"
    assert urls["https://cdn.example/video2.webm"] == "video"
    assert urls["https://cdn.example/thumb.jpg"] == "embed_media"


def test_extract_attachments_includes_inline_img_without_extension():
    inline_url = (
        "https://lh7-rt.googleusercontent.com/docsz/"
        "AD_4nXf3wCbMOYZbPufA7PzyPY46ITGJ3qtaPPMhXOSxltDikx42eU68kSIqxg4-woqhD8FMJG094gMAjYaOnW_UMWu3mjilsGFXTbSwGRES-k39gsY2fZH7h0hu12_MOehDVCdxB0QZnQ"
        "?key=S334lH3EYKYSJy0eMs4s3RSX"
    )
    payload = {"post": {"content": f'<p><br><img src="{inline_url}" title=""></p>'}}
    items = extract_attachments(payload)
    matches = [item for item in items if item.remote_url == inline_url]
    assert len(matches) == 1
    assert matches[0].kind == "inline_only"
    assert matches[0].name.endswith(".jpg")
