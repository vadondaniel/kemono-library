from kemono_library.kemono import KemonoPostRef, extract_attachments, fetch_post_json, parse_kemono_post_url


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
    assert captured_headers["Accept"].startswith("text/css")
