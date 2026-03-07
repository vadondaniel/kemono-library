from pathlib import Path

from kemono_library.web import create_app


def test_import_and_resolve_flow(tmp_path, monkeypatch):
    app = create_app(
        {
            "TESTING": True,
            "SECRET_KEY": "test",
            "DATABASE": str(tmp_path / "test.db"),
            "FILES_DIR": str(tmp_path / "files"),
        }
    )

    payload = {
        "title": "Post 100",
        "content": '<a href="https://kemono.cr/fanbox/post/101">linked</a>',
        "user": "70479526",
        "file": {"name": "cover.jpg", "path": "/data/x/cover.jpg"},
        "attachments": [],
    }

    def fake_fetch(ref, fallback_user_id=None):  # noqa: ARG001
        return payload

    def fake_download(remote_url, destination):  # noqa: ARG001
        Path(destination).parent.mkdir(parents=True, exist_ok=True)
        Path(destination).write_bytes(b"ok")

    monkeypatch.setattr("kemono_library.web.fetch_post_json", fake_fetch)
    monkeypatch.setattr("kemono_library.web.download_attachment", fake_download)

    client = app.test_client()

    creator_response = client.post("/creators", data={"name": "Creator A"}, follow_redirects=False)
    assert creator_response.status_code == 302
    assert "/creators/" in creator_response.headers["Location"]

    preview = client.post(
        "/import/preview",
        data={
            "post_url": "https://kemono.cr/fanbox/user/70479526/post/100",
            "creator_id": "1",
            "series_id": "",
        },
    )
    assert preview.status_code == 200
    assert b"cover.jpg" in preview.data

    commit = client.post(
        "/import/commit",
        data={
            "creator_id": "1",
            "series_id": "",
            "service": "fanbox",
            "user_id": "70479526",
            "post_id": "100",
            "selected_attachment": "0",
        },
        follow_redirects=False,
    )
    assert commit.status_code == 302
    assert commit.headers["Location"].endswith("/posts/1")

    detail = client.get("/posts/1")
    assert detail.status_code == 200
    assert b"/links/resolve?service=fanbox&amp;post=101" in detail.data

    unresolved = client.get("/links/resolve?service=fanbox&post=100&user=70479526")
    assert unresolved.status_code == 302
    assert unresolved.headers["Location"].endswith("/posts/1")
