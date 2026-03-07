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
        "post": {
            "title": "Post 100",
            "content": '<a href="https://kemono.cr/fanbox/post/101">linked</a>',
            "user": "70479526",
            "file": {"name": "cover.jpg", "path": "/data/x/cover.jpg"},
            "attachments": [],
            "published": "2025-10-25T12:00:00",
            "edited": "2025-10-25T12:00:00",
            "next": "200",
            "prev": "050",
            "tags": ["alpha", "beta"],
        },
        "attachments": [],
        "previews": [
            {
                "type": "thumbnail",
                "server": "https://n1.kemono.cr",
                "name": "cover-thumb.jpg",
                "path": "/x/y/thumb.jpg",
            },
            {
                "type": "thumbnail",
                "server": "https://n1.kemono.cr",
                "name": "cover-thumb-alias.jpg",
                "path": "/x/y/thumb.jpg",
            },
        ],
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
    attachments = app.db.list_attachments(1)  # type: ignore[attr-defined]
    assert attachments[0]["local_path"] == "post_1/cover.jpg"
    post = app.db.get_post(1)  # type: ignore[attr-defined]
    assert post["published_at"] == "2025-10-25T12:00:00"
    assert post["edited_at"] == "2025-10-25T12:00:00"
    assert post["next_external_post_id"] == "200"
    assert post["prev_external_post_id"] == "050"
    assert post["thumbnail_name"] == "cover.jpg"
    assert post["thumbnail_remote_url"] == "https://kemono.cr/data/x/cover.jpg"
    assert post["thumbnail_local_path"] == "post_1/cover.jpg"
    tags = app.db.list_tags(1)  # type: ignore[attr-defined]
    previews = app.db.list_previews(1)  # type: ignore[attr-defined]
    assert [row["tag"] for row in tags] == ["alpha", "beta"]
    assert len(previews) == 1
    assert previews[0]["name"] == "cover-thumb.jpg"

    detail = client.get("/posts/1")
    assert detail.status_code == 200
    assert b"/links/resolve?service=fanbox&amp;post=101" in detail.data
    assert b"Published:" in detail.data
    assert b"2025-10-25T12:00:00" in detail.data

    unresolved = client.get("/links/resolve?service=fanbox&post=100&user=70479526")
    assert unresolved.status_code == 302
    assert unresolved.headers["Location"].endswith("/posts/1")


def test_served_files_are_inline_not_forced_download(tmp_path):
    app = create_app(
        {
            "TESTING": True,
            "SECRET_KEY": "test",
            "DATABASE": str(tmp_path / "test.db"),
            "FILES_DIR": str(tmp_path / "files"),
        }
    )
    file_path = Path(app.config["FILES_DIR"]) / "post_1" / "img.jpg"
    file_path.parent.mkdir(parents=True, exist_ok=True)
    file_path.write_bytes(b"fakejpg")

    client = app.test_client()
    response = client.get("/files/post_1/img.jpg")

    assert response.status_code == 200
    disposition = response.headers.get("Content-Disposition", "")
    assert "attachment" not in disposition.lower()


def test_reimport_reuses_existing_file_without_duplicate_suffix(tmp_path, monkeypatch):
    app = create_app(
        {
            "TESTING": True,
            "SECRET_KEY": "test",
            "DATABASE": str(tmp_path / "test.db"),
            "FILES_DIR": str(tmp_path / "files"),
        }
    )

    payload = {
        "post": {
            "title": "Post Reimport",
            "content": "",
            "user": "70479526",
            "file": {"name": "cover.jpg", "path": "/data/x/cover.jpg"},
            "attachments": [],
        },
        "attachments": [],
    }

    download_calls = {"count": 0}

    def fake_fetch(ref, fallback_user_id=None):  # noqa: ARG001
        return payload

    def fake_download(remote_url, destination):  # noqa: ARG001
        download_calls["count"] += 1
        Path(destination).parent.mkdir(parents=True, exist_ok=True)
        Path(destination).write_bytes(b"ok")

    monkeypatch.setattr("kemono_library.web.fetch_post_json", fake_fetch)
    monkeypatch.setattr("kemono_library.web.download_attachment", fake_download)

    client = app.test_client()
    client.post("/creators", data={"name": "Creator A"}, follow_redirects=False)

    commit_data = {
        "creator_id": "1",
        "series_id": "",
        "service": "fanbox",
        "user_id": "70479526",
        "post_id": "100",
        "selected_attachment": "0",
    }
    first = client.post("/import/commit", data=commit_data, follow_redirects=False)
    second = client.post("/import/commit", data=commit_data, follow_redirects=False)

    assert first.status_code == 302
    assert second.status_code == 302
    assert download_calls["count"] == 1

    files = sorted((Path(app.config["FILES_DIR"]) / "post_1").glob("cover*.jpg"))
    assert len(files) == 1
    assert files[0].name == "cover.jpg"

    attachments = app.db.list_attachments(1)  # type: ignore[attr-defined]
    assert len(attachments) == 1
    assert attachments[0]["local_path"] == "post_1/cover.jpg"


def test_post_detail_maps_inline_alias_to_local_download(tmp_path):
    app = create_app(
        {
            "TESTING": True,
            "SECRET_KEY": "test",
            "DATABASE": str(tmp_path / "test.db"),
            "FILES_DIR": str(tmp_path / "files"),
        }
    )
    db = app.db  # type: ignore[attr-defined]

    creator_id = db.create_creator("Alias Creator")
    metadata = {
        "post": {
            "content": (
                '<p><a href="https://downloads.fanbox.cc/images/post/10791194/'
                'vh5E4UKF5F5EUzIIXGjVkRkc.jpeg" rel="noopener noreferrer"></a></p>'
            ),
            "attachments": [
                {"name": "eO8wzPLjankw59mg6YeTMzxN.jpeg", "path": "/c2/4f/hash.jpg"},
                {"name": "vh5E4UKF5F5EUzIIXGjVkRkc.jpeg", "path": "/c2/4f/hash.jpg"},
            ],
        }
    }
    post_id = db.upsert_post(
        creator_id=creator_id,
        series_id=None,
        service="fanbox",
        external_user_id="70479526",
        external_post_id="999",
        title="Alias Case",
        content=metadata["post"]["content"],
        metadata=metadata,
        source_url="https://kemono.cr/fanbox/user/70479526/post/999",
    )
    db.replace_attachments(
        post_id,
        [
            {
                "name": "eO8wzPLjankw59mg6YeTMzxN.jpeg",
                "remote_url": "https://kemono.cr/c2/4f/hash.jpg",
                "local_path": f"post_{post_id}/eO8wzPLjankw59mg6YeTMzxN.jpeg",
                "kind": "attachment",
            }
        ],
    )

    file_path = Path(app.config["FILES_DIR"]) / f"post_{post_id}" / "eO8wzPLjankw59mg6YeTMzxN.jpeg"
    file_path.parent.mkdir(parents=True, exist_ok=True)
    file_path.write_bytes(b"fakejpg")

    client = app.test_client()
    detail = client.get(f"/posts/{post_id}")
    assert detail.status_code == 200
    expected_local = f"/files/post_{post_id}/eO8wzPLjankw59mg6YeTMzxN.jpeg".encode()
    assert b'href="' + expected_local + b'"' in detail.data
    assert b'src="' + expected_local + b'"' in detail.data


def test_post_detail_prefers_attachment_over_inline_same_name(tmp_path):
    app = create_app(
        {
            "TESTING": True,
            "SECRET_KEY": "test",
            "DATABASE": str(tmp_path / "test.db"),
            "FILES_DIR": str(tmp_path / "files"),
        }
    )
    db = app.db  # type: ignore[attr-defined]
    creator_id = db.create_creator("Priority Creator")
    post_id = db.upsert_post(
        creator_id=creator_id,
        series_id=None,
        service="fanbox",
        external_user_id="70479526",
        external_post_id="1001",
        title="Priority Case",
        content=(
            '<p><a href="https://downloads.fanbox.cc/images/post/10791194/'
            'same-name.jpeg" rel="noopener noreferrer"></a></p>'
        ),
        metadata={
            "post": {
                "attachments": [{"name": "same-name.jpeg", "path": "/hashes/same-name.jpg"}]
            }
        },
        source_url="https://kemono.cr/fanbox/user/70479526/post/1001",
    )
    db.replace_attachments(
        post_id,
        [
            {
                "name": "same-name.jpeg",
                "remote_url": "https://downloads.fanbox.cc/images/post/10791194/same-name.jpeg",
                "local_path": f"post_{post_id}/inline-same-name.jpeg",
                "kind": "inline_media",
            },
            {
                "name": "same-name.jpeg",
                "remote_url": "https://kemono.cr/hashes/same-name.jpg",
                "local_path": f"post_{post_id}/attachment-same-name.jpeg",
                "kind": "attachment",
            },
        ],
    )

    files_root = Path(app.config["FILES_DIR"]) / f"post_{post_id}"
    files_root.mkdir(parents=True, exist_ok=True)
    (files_root / "inline-same-name.jpeg").write_bytes(b"inline")
    (files_root / "attachment-same-name.jpeg").write_bytes(b"attachment")

    client = app.test_client()
    detail = client.get(f"/posts/{post_id}")
    assert detail.status_code == 200
    expected = f"/files/post_{post_id}/attachment-same-name.jpeg".encode()
    assert b'href="' + expected + b'"' in detail.data
    assert b'src="' + expected + b'"' in detail.data


def test_edit_page_prettifies_html_content(tmp_path):
    app = create_app(
        {
            "TESTING": True,
            "SECRET_KEY": "test",
            "DATABASE": str(tmp_path / "test.db"),
            "FILES_DIR": str(tmp_path / "files"),
        }
    )
    db = app.db  # type: ignore[attr-defined]
    creator_id = db.create_creator("Editor Creator")
    post_id = db.upsert_post(
        creator_id=creator_id,
        series_id=None,
        service="fanbox",
        external_user_id="123",
        external_post_id="456",
        title="Edit Me",
        content="<p><strong>Hello</strong><br>World</p>",
        metadata={},
        source_url="https://kemono.cr/fanbox/user/123/post/456",
    )

    client = app.test_client()
    response = client.get(f"/posts/{post_id}/edit")

    assert response.status_code == 200
    assert b"<textarea id=\"content\" name=\"content\" rows=\"18\">" in response.data
    assert b"&lt;p&gt;" in response.data
    assert b"&lt;strong&gt;" in response.data
    assert b"Hello" in response.data
