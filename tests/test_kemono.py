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
    assert matching[0].kind == "attachment"
